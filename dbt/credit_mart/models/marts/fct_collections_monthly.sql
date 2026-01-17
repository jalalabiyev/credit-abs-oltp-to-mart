select
  date_trunc('month', p.payment_date)::date as month,
  l.product_type,
  p.currency,
  sum(p.amount_received) as collected_amount
from {{ ref('stg_payments') }} p
join {{ ref('stg_loan_contract') }} l using (loan_id)
group by 1,2,3
