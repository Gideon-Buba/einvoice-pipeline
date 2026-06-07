import "dotenv/config";
import { Pool } from "pg";
import * as sql from "mssql";
import { startApi } from "./api";
import {
  SOURCE_CONFIG,
  DEST_CONFIG,
  BATCH_SIZE,
  INSERT_CHUNK,
  CONCURRENCY,
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

// ─── HELPERS ─────────────────────────────────────────────────────────────────

function escapeStr(val: string | null): string {
  if (val === null || val === undefined) return "NULL";
  return `N'${val.replace(/'/g, "''")}'`;
}

function escapeDate(val: Date | null): string {
  if (val === null || val === undefined) return "NULL";
  try {
    const d = new Date(val);
    if (isNaN(d.getTime())) return "NULL";
    return `'${d.toISOString().slice(0, 10)}'`;
  } catch {
    return "NULL";
  }
}

function escapeNum(val: number | null): string {
  if (val === null || val === undefined || isNaN(Number(val))) return "NULL";
  return String(val);
}

// ─── INSERT ───────────────────────────────────────────────────────────────────
// Uses batched INSERT statements instead of bulk insert.
// Each chunk is a single INSERT ... VALUES (...), (...) statement.
// Regular INSERT keeps the TCP connection chatty — firewall won't kill it.

async function insertRows(
  destPool: sql.ConnectionPool,
  rows: InvoiceRow[],
): Promise<number> {
  let total = 0;

  for (let i = 0; i < rows.length; i += INSERT_CHUNK) {
    const chunk = rows
      .slice(i, i + INSERT_CHUNK)
      .filter((r): r is InvoiceRow & { id: number } => r.id !== null);
    if (chunk.length === 0) continue;

    const valueRows = chunk
      .map(
        (r) =>
          `(${[
            r.id,
            escapeStr(r.irn),
            escapeDate(r.issuedate),
            escapeDate(r.tax_point_date),
            escapeStr(r.document_currency_code),
            escapeStr(r.type_code),
            escapeStr(r.accounting_cost),
            escapeStr(r.payment_status),
            escapeStr(r.suppliertin),
            escapeStr(r.customertin),
            escapeNum(r.totaltaxamount),
            escapeNum(r.subtotaltaxamount),
            escapeNum(r.subtotaltaxableamount),
            escapeStr(r.taxcategoryid),
            escapeNum(r.taxcategorypercent),
            escapeDate(r.paymentduedate),
            escapeStr(r.paymentmeanscode),
            escapeNum(r.taxexclusiveamount),
            escapeNum(r.taxinclusiveamount),
            escapeNum(r.payableamount),
          ].join(",")})`,
      )
      .join(",\n");

    const sql_stmt = `
      INSERT INTO ${TABLE} (
        id, irn, issueDate, taxPointDate, currencyCode, invoiceTypeCode,
        accountingCost, paymentStatus, supplierTIN, customerTIN,
        totalTaxAmount, subtotalTaxAmount, subtotalTaxableAmount,
        taxCategoryId, taxCategoryPercent, paymentDueDate, paymentMeansCode,
        taxExclusiveAmount, taxInclusiveAmount, payableAmount
      )
      VALUES ${valueRows};
    `;

    try {
      const result = await destPool.request().query(sql_stmt);
      total += result.rowsAffected[0] ?? chunk.length;
    } catch (err) {
      const msg = (err as Error).message;
      if (
        msg.includes("PRIMARY KEY") ||
        msg.includes("duplicate") ||
        msg.includes("Violation of UNIQUE KEY")
      ) {
        // Fall back to row-by-row inserts to salvage non-duplicate rows
        for (const row of chunk) {
          try {
            const singleSql = `
              INSERT INTO ${TABLE} (
                id, irn, issueDate, taxPointDate, currencyCode, invoiceTypeCode,
                accountingCost, paymentStatus, supplierTIN, customerTIN,
                totalTaxAmount, subtotalTaxAmount, subtotalTaxableAmount,
                taxCategoryId, taxCategoryPercent, paymentDueDate, paymentMeansCode,
                taxExclusiveAmount, taxInclusiveAmount, payableAmount
              )
              VALUES (${[
                row.id,
                escapeStr(row.irn),
                escapeDate(row.issuedate),
                escapeDate(row.tax_point_date),
                escapeStr(row.document_currency_code),
                escapeStr(row.type_code),
                escapeStr(row.accounting_cost),
                escapeStr(row.payment_status),
                escapeStr(row.suppliertin),
                escapeStr(row.customertin),
                escapeNum(row.totaltaxamount),
                escapeNum(row.subtotaltaxamount),
                escapeNum(row.subtotaltaxableamount),
                escapeStr(row.taxcategoryid),
                escapeNum(row.taxcategorypercent),
                escapeDate(row.paymentduedate),
                escapeStr(row.paymentmeanscode),
                escapeNum(row.taxexclusiveamount),
                escapeNum(row.taxinclusiveamount),
                escapeNum(row.payableamount),
              ].join(",")});
            `;
            await destPool.request().query(singleSql);
            total += 1;
          } catch (rowErr) {
            const rowMsg = (rowErr as Error).message;
            if (
              !rowMsg.includes("PRIMARY KEY") &&
              !rowMsg.includes("duplicate") &&
              !rowMsg.includes("Violation of UNIQUE KEY")
            ) {
              throw rowErr;
            }
          }
        }
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
  sourcePool: Pool,
  destPool: sql.ConnectionPool,
  batchStart: number,
  batchEnd: number,
  attempt: number = 1,
): Promise<number> {
  try {
    log(`[${batchStart}] Querying (attempt ${attempt})`);

    const t0 = Date.now();
    const result = await sourcePool.query<InvoiceRow>(
      buildQuery(batchStart, batchEnd),
    );
    const rows = result.rows;
    const queryMs = Date.now() - t0;

    if (rows.length === 0) {
      log(`[${batchStart}] 0 rows — skipping`);
      return 0;
    }

    log(
      `[${batchStart}] ${rows.length.toLocaleString()} rows fetched in ${(queryMs / 1000).toFixed(1)}s — inserting...`,
    );

    const t1 = Date.now();
    const inserted = await insertRows(destPool, rows);
    const insertMs = Date.now() - t1;
    const rowsPerSecond = Math.round(inserted / (insertMs / 1000));

    log(
      `[${batchStart}] Done — ${inserted.toLocaleString()} rows in ${(insertMs / 1000).toFixed(1)}s (${rowsPerSecond.toLocaleString()} rows/s)`,
    );

    appendMetrics({
      batchStart,
      batchEnd,
      rowsInserted: inserted,
      durationMs: insertMs,
      rowsPerSecond,
      completedAt: new Date().toISOString(),
    });

    return inserted;
  } catch (err) {
    const message = (err as Error).message;
    log(`[${batchStart}] Failed (attempt ${attempt}): ${message}`);

    if (attempt < MAX_RETRIES) {
      await sleep(RETRY_DELAY_MS);
      return processBatch(
        sourcePool,
        destPool,
        batchStart,
        batchEnd,
        attempt + 1,
      );
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

// ─── WORKER POOL ─────────────────────────────────────────────────────────────

async function runWorkerPool(
  sourcePool: Pool,
  destPool: sql.ConnectionPool,
  batches: { batchStart: number; batchEnd: number }[],
  progress: Progress,
  resumeFrom: number,
): Promise<Progress> {
  const queue = [...batches];
  const completedStarts = new Set<number>();
  let totalRowsInserted = progress.totalRowsInserted;
  let totalBatchesCompleted = progress.totalBatchesCompleted;

  const worker = async (): Promise<void> => {
    while (true) {
      const batch = queue.shift();
      if (!batch) break;

      const { batchStart, batchEnd } = batch;
      const inserted = await processBatch(
        sourcePool,
        destPool,
        batchStart,
        batchEnd,
      );

      if (inserted >= 0) {
        completedStarts.add(batchStart);
        totalRowsInserted += inserted;
        totalBatchesCompleted += 1;

        let highestContiguous = resumeFrom - BATCH_SIZE;
        let cursor = resumeFrom;
        while (completedStarts.has(cursor)) {
          highestContiguous = cursor;
          cursor += BATCH_SIZE;
        }

        const updatedProgress: Progress = {
          ...progress,
          lastCompletedBatchStart: highestContiguous,
          totalBatchesCompleted,
          totalRowsInserted,
          lastUpdatedAt: new Date().toISOString(),
        };

        saveProgress(updatedProgress);
        progress = updatedProgress;

        await sendTelegram(
          `✅ *Batch Complete*\nRange: ${batchStart.toLocaleString()} → ${batchEnd.toLocaleString()}\nInserted: ${inserted.toLocaleString()}\nTotal batches: ${totalBatchesCompleted}\nTotal rows: ${totalRowsInserted.toLocaleString()}`,
        );
      } else {
        await sendTelegram(
          `❌ *Batch Failed*\nRange: ${batchStart.toLocaleString()} → ${batchEnd.toLocaleString()}\nCheck failed_ranges.json`,
        );
      }
    }
  };

  await Promise.all(Array.from({ length: CONCURRENCY }, worker));
  return progress;
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  ensureDirs();
  startApi();

  const sourcePool = new Pool(SOURCE_CONFIG);
  sourcePool.on("error", (err) => log(`pg pool error: ${err.message}`));

  const { rows } = await sourcePool.query<{ max_id: string }>(
    "SELECT MAX(id)::text AS max_id FROM invoices",
  );
  const MAX_ID = parseInt(rows[0].max_id, 10);
  log(`Source max ID: ${MAX_ID.toLocaleString()}`);

  log("Connecting to NRS_EDW...");
  const destPool = await sql.connect(DEST_CONFIG);
  log(`Connected to NRS_EDW (pool size: ${CONCURRENCY})`);

  let progress: Progress = loadProgress() ?? {
    lastCompletedBatchStart: MIN_ID - BATCH_SIZE,
    totalBatchesCompleted: 0,
    totalRowsInserted: 0,
    startedAt: new Date().toISOString(),
    lastUpdatedAt: new Date().toISOString(),
  };

  const resumeFrom = progress.lastCompletedBatchStart + BATCH_SIZE;
  log(
    `Resuming from ID ${resumeFrom.toLocaleString()} with ${CONCURRENCY} concurrent workers`,
  );

  const batches: { batchStart: number; batchEnd: number }[] = [];
  for (let s = resumeFrom; s <= MAX_ID; s += BATCH_SIZE) {
    batches.push({
      batchStart: s,
      batchEnd: Math.min(s + BATCH_SIZE - 1, MAX_ID),
    });
  }

  log(`Total batches to process: ${batches.length.toLocaleString()}`);

  await sendTelegram(
    `🚀 *Pipeline Started*\nMax ID: ${MAX_ID.toLocaleString()}\nResuming from: ${resumeFrom.toLocaleString()}\nBatches remaining: ${batches.length}\nConcurrency: ${CONCURRENCY} workers`,
  );

  const finalProgress = await runWorkerPool(
    sourcePool,
    destPool,
    batches,
    progress,
    resumeFrom,
  );

  const failed = loadFailed();
  await sendTelegram(
    `🏁 *Pipeline Complete*\nTotal batches: ${finalProgress.totalBatchesCompleted}\nTotal rows: ${finalProgress.totalRowsInserted.toLocaleString()}\nFailed ranges: ${failed.length}`,
  );

  await sourcePool.end();
  await destPool.close();
}

main().catch(async (err) => {
  const msg = `💥 Pipeline crashed: ${(err as Error).message}`;
  log(msg);
  await sendTelegram(msg);
  process.exit(1);
});
