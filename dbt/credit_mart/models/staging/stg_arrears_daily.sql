select
  loan_id::bigint as loan_id,
  as_of_date::date as as_of_date,
  days_past_due::int as days_past_due,
  past_due_amount_total::numeric as past_due_amount_total,
  past_due_principal::numeric as past_due_principal,
  past_due_interest::numeric as past_due_interest,
  past_due_fees::numeric as past_due_fees,
  oldest_unpaid_due_date::date as oldest_unpaid_due_date,
  early_arrears_flag::boolean as early_arrears_flag,
  default_flag::boolean as default_flag,
  nonperforming_flag::boolean as nonperforming_flag,

  case
    when days_past_due <= 0 then '0'
    when days_past_due between 1 and 30 then '1-30'
    when days_past_due between 31 and 60 then '31-60'
    when days_past_due between 61 and 90 then '61-90'
    else '90+'
  end as dpd_bucket,

  (coalesce(nonperforming_flag,false) or coalesce(default_flag,false) or days_past_due > 90) as npl_flag
from {{ source('credit_oltp', 'arrears_dpd_status') }}