select
  loan_id::bigint as loan_id,
  borrower_id::bigint as borrower_id,
  application_id::bigint as application_id,
  product_type,
  currency,
  origination_date::date as origination_date,
  disbursement_date::date as disbursement_date,
  maturity_date::date as maturity_date,
  principal_original::numeric as principal_original,
  principal_current::numeric as principal_current,
  term_months::int as term_months,
  interest_rate_type,
  interest_rate_current::numeric as interest_rate_current,
  repayment_method,
  payment_frequency,
  grace_period_months::int as grace_period_months,
  status
from {{ source('credit_oltp', 'loan_contract') }}
