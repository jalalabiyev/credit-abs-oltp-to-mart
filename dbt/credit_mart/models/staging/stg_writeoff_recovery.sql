select
  loan_id::bigint as loan_id,
  writeoff_date::date as writeoff_date,
  writeoff_amount_principal::numeric as writeoff_amount_principal,
  writeoff_amount_interest::numeric as writeoff_amount_interest,
  writeoff_amount_fees::numeric as writeoff_amount_fees,
  recovery_amount::numeric as recovery_amount,
  recovery_date::date as recovery_date
from {{ source('credit_oltp', 'write_off_and_recovery') }}