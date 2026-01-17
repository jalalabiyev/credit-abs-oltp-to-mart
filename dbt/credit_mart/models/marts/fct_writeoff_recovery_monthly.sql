select
  date_trunc('month', coalesce(recovery_date, writeoff_date))::date as month,
  sum(coalesce(writeoff_amount_principal,0) + coalesce(writeoff_amount_interest,0) + coalesce(writeoff_amount_fees,0)) as writeoff_total,
  sum(coalesce(recovery_amount,0)) as recovery_total
from {{ ref('stg_writeoff_recovery') }}
group by 1
