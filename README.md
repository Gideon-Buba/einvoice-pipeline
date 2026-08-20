# E-Invoice Pipeline

A batched, resumable ETL pipeline that migrates invoice data from a
**PostgreSQL** e-invoicing source database into a **SQL Server** data
warehouse (`dbo.factInvoice`). It ships with a live status dashboard,
Telegram alerts, and automatic retry/resume so long-running migrations
survive restarts, network blips, and partial failures.

```
┌──────────────┐   batched SELECT    ┌──────────────┐   batched INSERT   ┌──────────────┐
│  PostgreSQL  │ ───────────────────▶│   pipeline   │───────────────────▶│  SQL Server  │
│  (source)    │   id BETWEEN x..y   │  (worker     │  INSERT...VALUES   │ dbo.factInv  │
│  invoices...  │                     │   pool)      │   in chunks        │  (dest)      │
└──────────────┘                     └──────┬───────┘                    └──────────────┘
                                             │
                              progress.json / failed_ranges.json / metrics.json
                                             │
                                   ┌─────────┴─────────┐
                                   │  Express dashboard │  http://localhost:3001
                                   │  + Telegram alerts │
                                   └────────────────────┘
```

---

## Quick start

```bash
git clone <this-repo>
cd einvoice-pipeline
npm install
cp .env.example .env   # then fill in real credentials
npm run dev             # ts-node, no build step — good for local dev
```

For a production-style run:

```bash
npm run build   # compiles src/ -> dist/ with tsc
npm run start   # runs dist/pipeline.js
```

Once running, open **http://localhost:3001** for the live status dashboard
(rows inserted, throughput, ETA, failed ranges, etc).

### Requirements

- Node.js 18+ (uses native `fetch`)
- Network access to both the source Postgres instance and destination SQL
  Server instance
- A Postgres role with `SELECT` on `invoices` and its related tables
- A SQL Server login with `INSERT`/`SELECT` on `dbo.factInvoice`

---

## How it works

### 1. ID-range batching

The pipeline doesn't page through the source table with `OFFSET/LIMIT`
(slow at scale on large tables). Instead, on startup it:

1. Runs `SELECT MAX(id) FROM invoices` to find the highest source ID.
2. Splits the range `[MIN_ID, MAX_ID]` into fixed-size chunks of
   `BATCH_SIZE` (default **250,000**) ids each — e.g. `5–250004`,
   `250005–500009`, etc.
3. Feeds those `{batchStart, batchEnd}` ranges into a worker pool.

This means batching is driven by **primary key ranges**, not row counts —
if a range has gaps (deleted rows), that batch just returns fewer rows,
which is fine.

### 2. The source query ([`src/pipeline.ts`](src/pipeline.ts) — `buildQuery`)

For each batch, one query is run against Postgres:

```sql
SELECT
  i.id, i.irn, i.issue_date AS issuedate, i.tax_point_date,
  i.document_currency_code, i.type_code, i.accounting_cost, i.payment_status,
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
LEFT JOIN invoice_rel_parties supplier_rel ON ... kind = 'supplier' ...
LEFT JOIN invoice_parties supplier ON supplier.id = supplier_rel.parties_id ...
LEFT JOIN invoice_rel_parties customer_rel ON ... kind = 'customer' ...
LEFT JOIN invoice_parties customer ON customer.id = customer_rel.parties_id ...
LEFT JOIN invoice_monetary_totals mt ON mt.invoice_id = i.id ...
LEFT JOIN LATERAL (... first invoice_payment_means row, ordered by id ...) first_payment ON true
LEFT JOIN LATERAL (... invoice_tax_totals joined to STANDARD_VAT sub-total ...) vat_tax ON true
WHERE i.deleted_at IS NULL AND i.id BETWEEN <batchStart> AND <batchEnd>;
```

What this is doing, table by table:

| Join | Purpose |
|---|---|
| `invoice_rel_parties` + `invoice_parties` (×2, aliased `supplier`/`customer`) | Resolves the invoice's supplier and customer party rows via a kind-tagged relation table, and pulls each party's TIN (tax ID number). |
| `invoice_monetary_totals` (`mt`) | Pulls the invoice's tax-exclusive, tax-inclusive, and payable amounts. |
| `invoice_payment_means` (LATERAL `first_payment`) | Grabs only the **first** payment means row per invoice (by lowest id) — an invoice can have multiple payment methods, but the warehouse fact table stores one. |
| `invoice_tax_totals` + `invoice_tax_sub_totals` (LATERAL `vat_tax`) | Grabs the **STANDARD_VAT** tax sub-total specifically (filtered via `tax_category_id = 'STANDARD_VAT'`) — other tax categories on the invoice are ignored. |

All joins are `LEFT JOIN`/`LEFT JOIN LATERAL` so an invoice with missing
supplier/customer/payment/tax data still comes through with `NULL`s rather
than being dropped. Every joined table is filtered on `deleted_at IS NULL`
(soft-delete aware).

**To change what's synced:** edit `buildQuery()` in
[`src/pipeline.ts`](src/pipeline.ts). If you add/remove a column, also update:
- The `InvoiceRow` interface in [`src/types.ts`](src/types.ts)
- The column list and value-mapping in `insertRows()` (see below)
- The destination table schema (`dbo.factInvoice`)

### 3. Destination writes ([`src/pipeline.ts`](src/pipeline.ts) — `insertRows`)

Rows aren't bulk-loaded — they're written as plain batched
`INSERT INTO dbo.factInvoice (...) VALUES (...), (...), ...` statements,
`INSERT_CHUNK` rows at a time (default **5,000**). This is intentional: a
single long-lived bulk copy stream is more likely to be killed by strict
corporate firewalls/proxies on long transfers, whereas regular chunked
`INSERT`s keep the TCP connection "chatty" and resilient.

Values are manually escaped before being interpolated into the SQL string
(see `escapeStr`, `escapeDate`, `escapeNum` in `pipeline.ts`) rather than
using parameterized queries, because `mssql`'s parameterized API doesn't
scale well to multi-thousand-row multi-row `VALUES` statements. Strings are
wrapped as SQL Server `N'...'` (nvarchar) literals with `'` doubled for
escaping; dates are normalized to `YYYY-MM-DD`; numbers are validated with
`isNaN` and fall back to `NULL`.

**Duplicate-key handling:** if a chunk insert fails with a primary-key /
unique-constraint violation, the pipeline automatically falls back to
inserting that chunk **row by row**, silently skipping only the rows that
individually violate the constraint (e.g. because they were already
migrated in a previous run) — everything else in the chunk still gets
inserted. Any other kind of error propagates and triggers the batch retry
logic below.

### 4. Retries, resume, and failure tracking

- Each batch is retried up to `MAX_RETRIES` (**3**, hardcoded) times with a
  `RETRY_DELAY_MS` (**10s**, hardcoded) pause between attempts, on any
  error (query timeout, connection drop, etc).
- If a batch still fails after all retries, its range is recorded in
  `failed_ranges.json` (in `LOGS_DIR`) instead of crashing the whole run.
- Progress is checkpointed after every successfully completed batch to
  `progress.json`, tracking the **highest contiguous completed range** —
  so if the process is killed and restarted, it resumes from
  `lastCompletedBatchStart + BATCH_SIZE` rather than re-scanning everything
  or silently skipping gaps.
- `MAX_RETRIES`, `RETRY_DELAY_MS`, `TABLE`, and `MIN_ID` are hardcoded
  constants in [`src/config.ts`](src/config.ts) (not env-driven) — edit
  them there if you need different retry behavior, a different target
  table, or a different starting ID.

### 5. Concurrency

Batches are processed by a small in-memory worker pool
(`runWorkerPool` in `pipeline.ts`): `CONCURRENCY` workers each pull the
next batch off a shared queue and process it independently, sharing one
Postgres pool and one SQL Server pool. Raising `CONCURRENCY` increases
throughput but also increases load on both databases — keep it at or below
your SQL Server connection pool limits (the destination pool size is tied
to `CONCURRENCY`, see `DEST_CONFIG.pool.max` in `config.ts`).

### 6. Status dashboard & API ([`src/api.ts`](src/api.ts))

An Express server starts alongside the pipeline (default port **3001**,
hardcoded — change `PORT` in `api.ts` if needed) and exposes:

- `GET /` — a dark/light themed HTML dashboard (auto-refreshes every 30s)
  showing rows inserted, ingestion %, batches completed, failed ranges,
  live throughput, a speed sparkline, and an ETA.
- `GET /api/status` — the JSON the dashboard polls. It independently
  connects to both databases on each request to compute:
  - `totalRowsInserted` — live `COUNT(*)` on `dbo.factInvoice`
  - `totalSourceRows` — live `MAX(id)` on `invoices`
  - `avgRowsPerSecond` — rolling average over the last 10 batches
    (from `metrics.json`)
  - `etaMs` — projected completion time from current throughput and rows
    remaining

The dashboard runs independently of the migration loop — it works even if
the pipeline itself has crashed (it'll just show connection errors for
whichever side is unreachable).

### 7. Telegram notifications ([`src/telegram.ts`](src/telegram.ts))

If `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` are set, the pipeline posts
messages to that chat via the Telegram Bot API on:

- Pipeline start (max ID, resume point, batch count, concurrency)
- Each completed batch (range, rows inserted, running totals)
- Each batch that exhausts its retries and fails
- Pipeline completion (totals + failed range count)
- Unhandled crashes (`main().catch(...)` in `pipeline.ts`)

Failures to reach Telegram are caught and logged, never thrown — a
Telegram outage will never crash the pipeline. See `.env.example` for
step-by-step bot setup instructions.

### 8. Logging & local state ([`src/logger.ts`](src/logger.ts), [`src/store.ts`](src/store.ts))

Everything is written under `LOGS_DIR`:

| File | Contents |
|---|---|
| `pipeline_YYYY-MM-DD.log` | Plain-text log, one file per day, every `log()` call — also echoed to stdout |
| `progress.json` | `{ lastCompletedBatchStart, totalBatchesCompleted, totalRowsInserted, startedAt, lastUpdatedAt }` — read on startup to resume |
| `failed_ranges.json` | Array of `{ batchStart, batchEnd, reason, failedAt, attempts }` for batches that exhausted retries |
| `metrics.json` | Rolling window (last 20) of `{ batchStart, batchEnd, rowsInserted, durationMs, rowsPerSecond, completedAt }`, used for the dashboard sparkline/ETA |

---

## Configuration reference

All runtime config lives in [`src/config.ts`](src/config.ts) and is sourced
from environment variables (loaded via `dotenv/config`) — see
[`.env.example`](.env.example) for the full list with descriptions. A few
values are **hardcoded** rather than env-driven (edit `config.ts` directly
to change them):

| Constant | Value | Meaning |
|---|---|---|
| `MAX_RETRIES` | `3` | Retry attempts per failed batch |
| `RETRY_DELAY_MS` | `10_000` | Delay between retry attempts |
| `TABLE` | `"dbo.factInvoice"` | Destination table name |
| `MIN_ID` | `5` | Lowest source `id` the pipeline will consider on a fresh run |

And in [`src/api.ts`](src/api.ts):

| Constant | Value | Meaning |
|---|---|---|
| `PORT` | `3001` | Dashboard/API port |

---

## What's safe to change vs. what needs care

**Safe to tune via `.env`:** `BATCH_SIZE`, `INSERT_CHUNK`, `CONCURRENCY`,
`PG_POOL_SIZE`, `LOGS_DIR`.

**Needs care:**
- Changing `TABLE`, `MIN_ID`, `MAX_RETRIES`, `RETRY_DELAY_MS` in
  `config.ts` requires a rebuild (`npm run build`) if you're running the
  compiled `dist/` version.
- Changing the column set in `buildQuery()` / `insertRows()` must be kept
  in sync with `InvoiceRow` (`types.ts`) and the actual `dbo.factInvoice`
  schema, or inserts will fail.
- `CONCURRENCY` is also used to size the destination SQL Server pool
  (`DEST_CONFIG.pool.max`) — raising it without checking your SQL Server's
  max connection limit can start failing connections under load.
- If you delete or rename `LOGS_DIR`'s `progress.json`, the next run will
  restart from `MIN_ID` and reprocess everything (safe due to the
  duplicate-key fallback in `insertRows`, but slower).

---

## Scripts

| Command | Description |
|---|---|
| `npm run dev` | Run directly from TypeScript via `ts-node` (no build step) |
| `npm run build` | Type-check and compile `src/` → `dist/` via `tsc` |
| `npm run start` | Run the compiled pipeline from `dist/pipeline.js` |

---

## Project structure

```
src/
  pipeline.ts   # main entrypoint: batching, query, insert, retry, worker pool
  config.ts     # env-driven + hardcoded configuration
  types.ts      # shared TypeScript interfaces
  store.ts      # progress/failed-range/metrics persistence (JSON files)
  logger.ts     # console + daily log-file writer
  telegram.ts   # Telegram Bot API notifier
  api.ts        # Express status dashboard + /api/status endpoint
assets/
  nrs-logo.jpg  # dashboard logo
```
