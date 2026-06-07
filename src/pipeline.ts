import "dotenv/config";
import { Client } from "pg";
import * as sql from "mssql";
import { startApi } from "./api";
import {
  SOURCE_CONFIG,
  DEST_CONFIG,
  BATCH_SIZE,
  INSERT_CHUNK,
  MAX_RETRIES,
  RETRY_DELAY_MS,
  TABLE,
  MIN_ID,
} from "./config";
import { InvoiceRow, Progress } from "./types";
import { log } from "./logger";
import { sendTelegram } from "./telegram";
import {
  ensureDirs,
  loadProgress,
  saveProgress,
  loadFailed,
  appendFailed,
  appendMetrics,
} from "./store";

// ─── QUERY ───────────────────────────────────────────────────────────────────

function buildQuery(batchStart: number, batchEnd: number): string {
  return `
    SELECT
      i.id, i.irn,
      i.issue_date AS issuedate,
      i.tax_point_date,
      i.document_currency_code,
      i.type_code,
      i.accounting_cost,
      i.payment_status,
      supplier.tin AS suppliertin,
      customer.tin AS customertin,
      vat_tax.amount AS totaltaxamount,
      vat_tax.subtotal_amount AS subtotaltaxamount,
      vat_tax.taxable_amount AS subtotaltaxableamount,
      vat_tax.tax_category_id AS taxcategoryid,
      vat_tax.tax_category_percent AS taxcategorypercent,
      first_payment.due_date AS paymentduedate,
      first_payment.code AS paymentmeanscode,
      mt.tax_exclusive_amount AS taxexclusiveamount,
      mt.tax_inclusive_amount AS taxinclusiveamount,
      mt.payable_amount AS payableamount
    FROM invoices i
    LEFT JOIN invoice_rel_parties supplier_rel ON supplier_rel.invoice_id = i.id AND supplier_rel.kind = 'supplier' AND supplier_rel.deleted_at IS NULL
    LEFT JOIN invoice_parties supplier ON supplier.id = supplier_rel.parties_id AND supplier.deleted_at IS NULL
    LEFT JOIN invoice_rel_parties customer_rel ON customer_rel.invoice_id = i.id AND customer_rel.kind = 'customer' AND customer_rel.deleted_at IS NULL
    LEFT JOIN invoice_parties customer ON customer.id = customer_rel.parties_id AND customer.deleted_at IS NULL
    LEFT JOIN invoice_monetary_totals mt ON mt.invoice_id = i.id AND mt.deleted_at IS NULL
    LEFT JOIN LATERAL (
      SELECT pm.due_date, pm.code FROM invoice_payment_means pm
      WHERE pm.invoice_id = i.id AND pm.deleted_at IS NULL ORDER BY pm.id LIMIT 1
    ) first_payment ON true
    LEFT JOIN LATERAL (
      SELECT tt.amount, tst.amount AS subtotal_amount, tst.taxable_amount, tst.tax_category_id, tst.tax_category_percent
      FROM invoice_tax_totals tt
      JOIN invoice_tax_sub_totals tst ON tst.tax_id = tt.id AND tst.deleted_at IS NULL AND tst.tax_category_id = 'STANDARD_VAT'
      WHERE tt.invoice_id = i.id AND tt.deleted_at IS NULL ORDER BY tt.id, tst.id LIMIT 1
    ) vat_tax ON true
    WHERE i.deleted_at IS NULL AND i.id BETWEEN ${batchStart} AND ${batchEnd};
  `;
}

// ─── INSERT ───────────────────────────────────────────────────────────────────

async function insertRows(
  pool: sql.ConnectionPool,
  rows: InvoiceRow[],
): Promise<number> {
  let total = 0;
  for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
    const chunk = rows
      .slice(i, i + INSERT_CHUNK)
      .filter((r): r is InvoiceRow & { id: number } => r.id !== null);
    if (chunk.length === 0) continue;

    const table = new sql.Table(TABLE);
    table.create = false;
    table.columns.add("id", sql.Int, { nullable: false });
    table.columns.add("irn", sql.NVarChar(100), { nullable: true });
    table.columns.add("issueDate", sql.Date, { nullable: true });
    table.columns.add("taxPointDate", sql.Date, { nullable: true });
    table.columns.add("currencyCode", sql.NVarChar(50), { nullable: true });
    table.columns.add("invoiceTypeCode", sql.NVarChar(50), { nullable: true });
    table.columns.add("accountingCost", sql.NVarChar(500), { nullable: true });
    table.columns.add("paymentStatus", sql.NVarChar(50), { nullable: true });
    table.columns.add("supplierTIN", sql.NVarChar(50), { nullable: true });
    table.columns.add("customerTIN", sql.NVarChar(50), { nullable: true });
    table.columns.add("totalTaxAmount", sql.Numeric(16, 2), { nullable: true });
    table.columns.add("subtotalTaxAmount", sql.Numeric(16, 2), {
      nullable: true,
    });
    table.columns.add("subtotalTaxableAmount", sql.Numeric(16, 2), {
      nullable: true,
    });
    table.columns.add("taxCategoryId", sql.NVarChar(100), { nullable: true });
    table.columns.add("taxCategoryPercent", sql.Numeric(6, 2), {
      nullable: true,
    });
    table.columns.add("paymentDueDate", sql.Date, { nullable: true });
    table.columns.add("paymentMeansCode", sql.NVarChar(50), { nullable: true });
    table.columns.add("taxExclusiveAmount", sql.Numeric(16, 2), {
      nullable: true,
    });
    table.columns.add("taxInclusiveAmount", sql.Numeric(16, 2), {
      nullable: true,
    });
    table.columns.add("payableAmount", sql.Numeric(16, 2), { nullable: true });

    for (const row of chunk) {
      table.rows.add(
        row.id,
        row.irn,
        row.issuedate,
        row.tax_point_date,
        row.document_currency_code,
        row.type_code,
        row.accounting_cost,
        row.payment_status,
        row.suppliertin,
        row.customertin,
        row.totaltaxamount,
        row.subtotaltaxamount,
        row.subtotaltaxableamount,
        row.taxcategoryid,
        row.taxcategorypercent,
        row.paymentduedate,
        row.paymentmeanscode,
        row.taxexclusiveamount,
        row.taxinclusiveamount,
        row.payableamount,
      );
    }

    try {
      const result = await pool.request().bulk(table);
      total += result.rowsAffected;
    } catch (err) {
      const msg = (err as Error).message;
      if (msg.includes("PRIMARY KEY") || msg.includes("duplicate")) {
        log(`Duplicate keys in chunk ${i} — skipped`);
      } else {
        throw err;
      }
    }
  }
  return total;
}

// ─── PROCESS BATCH ────────────────────────────────────────────────────────────

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function processBatch(
  destPool: sql.ConnectionPool,
  batchStart: number,
  batchEnd: number,
  attempt: number = 1,
): Promise<number> {
  const source = new Client(SOURCE_CONFIG);
  try {
    await source.connect();
    log(`Querying ${batchStart} → ${batchEnd} (attempt ${attempt})`);

    const t0: number = Date.now();
    const result = await source.query<InvoiceRow>(
      buildQuery(batchStart, batchEnd),
    );
    const rows = result.rows;
    await source.end();

    if (rows.length === 0) {
      log(`Batch ${batchStart}-${batchEnd} — 0 rows, skipping`);
      return 0;
    }

    log(
      `${rows.length.toLocaleString()} rows fetched in ${((Date.now() - t0) / 1000).toFixed(1)}s, inserting...`,
    );

    const t1: number = Date.now();
    const inserted = await insertRows(destPool, rows);
    const durationMs = Date.now() - t1;
    const rowsPerSecond = Math.round(inserted / (durationMs / 1000));

    log(
      `Batch ${batchStart}-${batchEnd} done — ${inserted.toLocaleString()} rows inserted in ${(durationMs / 1000).toFixed(1)}s (${rowsPerSecond.toLocaleString()} rows/s)`,
    );

    // Save metrics for dashboard
    appendMetrics({
      batchStart,
      batchEnd,
      rowsInserted: inserted,
      durationMs,
      rowsPerSecond,
      completedAt: new Date().toISOString(),
    });

    return inserted;
  } catch (err) {
    await source.end().catch(() => {});
    const message = (err as Error).message;
    log(
      `Batch ${batchStart}-${batchEnd} failed (attempt ${attempt}): ${message}`,
    );

    if (attempt < MAX_RETRIES) {
      await sleep(RETRY_DELAY_MS);
      return processBatch(destPool, batchStart, batchEnd, attempt + 1);
    }

    appendFailed({
      batchStart,
      batchEnd,
      reason: message,
      failedAt: new Date().toISOString(),
      attempts: attempt,
    });
    return -1;
  }
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  ensureDirs();
  startApi();

  const sourceClient = new Client(SOURCE_CONFIG);
  await sourceClient.connect();
  const maxResult = await sourceClient.query<{ max_id: string }>(
    "SELECT MAX(id)::text AS max_id FROM invoices",
  );
  const MAX_ID = parseInt(maxResult.rows[0].max_id, 10);
  await sourceClient.end();
  log(`Source max ID: ${MAX_ID.toLocaleString()}`);

  log("Connecting to NRS_EDW...");
  const destPool = await sql.connect(DEST_CONFIG);
  log("Connected to NRS_EDW");

  let progress: Progress = loadProgress() ?? {
    lastCompletedBatchStart: MIN_ID - BATCH_SIZE,
    totalBatchesCompleted: 0,
    totalRowsInserted: 0,
    startedAt: new Date().toISOString(),
    lastUpdatedAt: new Date().toISOString(),
  };

  const resumeFrom = progress.lastCompletedBatchStart + BATCH_SIZE;
  log(`Resuming from ID ${resumeFrom.toLocaleString()}`);

  await sendTelegram(
    `🚀 *Pipeline Started*\nMax ID: ${MAX_ID.toLocaleString()}\nResuming from: ${resumeFrom.toLocaleString()}\nBatches done: ${progress.totalBatchesCompleted}`,
  );

  for (
    let batchStart = resumeFrom;
    batchStart <= MAX_ID;
    batchStart += BATCH_SIZE
  ) {
    const batchEnd = Math.min(batchStart + BATCH_SIZE - 1, MAX_ID);
    const inserted = await processBatch(destPool, batchStart, batchEnd);

    if (inserted >= 0) {
      progress.lastCompletedBatchStart = batchStart;
      progress.totalBatchesCompleted += 1;
      progress.totalRowsInserted += inserted;
      progress.lastUpdatedAt = new Date().toISOString();
      saveProgress(progress);

      await sendTelegram(
        `✅ *Batch Complete*\nRange: ${batchStart.toLocaleString()} → ${batchEnd.toLocaleString()}\nInserted: ${inserted.toLocaleString()}\nTotal batches: ${progress.totalBatchesCompleted}\nTotal rows: ${progress.totalRowsInserted.toLocaleString()}`,
      );
    } else {
      await sendTelegram(
        `❌ *Batch Failed*\nRange: ${batchStart.toLocaleString()} → ${batchEnd.toLocaleString()}\nCheck failed_ranges.json`,
      );
    }
  }

  const failed = loadFailed();
  await sendTelegram(
    `🏁 *Pipeline Complete*\nTotal batches: ${progress.totalBatchesCompleted}\nTotal rows: ${progress.totalRowsInserted.toLocaleString()}\nFailed ranges: ${failed.length}`,
  );

  await destPool.close();
}

main().catch(async (err) => {
  const msg = `💥 Pipeline crashed: ${(err as Error).message}`;
  log(msg);
  await sendTelegram(msg);
  process.exit(1);
});
