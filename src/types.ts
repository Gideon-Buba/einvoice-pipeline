export interface Progress {
  lastCompletedBatchStart: number;
  totalBatchesCompleted: number;
  totalRowsInserted: number;
  startedAt: string;
  lastUpdatedAt: string;
}

export interface FailedRange {
  batchStart: number;
  batchEnd: number;
  reason: string;
  failedAt: string;
  attempts: number;
}

export interface InvoiceRow {
  id: number | null;
  irn: string | null;
  issuedate: Date | null;
  tax_point_date: Date | null;
  document_currency_code: string | null;
  type_code: string | null;
  accounting_cost: string | null;
  payment_status: string | null;
  suppliertin: string | null;
  customertin: string | null;
  totaltaxamount: number | null;
  subtotaltaxamount: number | null;
  subtotaltaxableamount: number | null;
  taxcategoryid: string | null;
  taxcategorypercent: number | null;
  paymentduedate: Date | null;
  paymentmeanscode: string | null;
  taxexclusiveamount: number | null;
  taxinclusiveamount: number | null;
  payableamount: number | null;
}
