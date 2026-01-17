select
  a.as_of_date,
  a.loan_id,
  l.borrower_id,
  l.product_type,
  l.currency,
  l.origination_date,
  l.principal_current as exposure,
  a.days_past_due,
  a.dpd_bucket,
  a.npl_flag,
  a.past_due_amount_total
from {{ ref('stg_arrears_daily') }} a
join {{ ref('stg_loan_contract') }} l using (loan_id)
