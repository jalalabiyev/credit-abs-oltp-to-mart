select
  payment_id::bigint as payment_id,
  loan_id::bigint as loan_id,
  payment_date::date as payment_date,
  value_date::date as value_date,
  currency,
  amount_received::numeric as amount_received,
  payment_channel,
  external_reference,
  status
from {{ source('credit_oltp', 'repayment_payment') }}