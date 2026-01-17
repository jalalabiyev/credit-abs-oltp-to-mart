with month_end as (
  select *
  from (
    select
      loan_id,
      date_trunc('month', as_of_date)::date as month,
      dpd_bucket,
      row_number() over (partition by loan_id, date_trunc('month', as_of_date) order by as_of_date desc) as rn
    from {{ ref('stg_arrears_daily') }}
  ) x
  where rn = 1
),
x as (
  select
    loan_id,
    month,
    lag(dpd_bucket) over (partition by loan_id order by month) as prev_bucket,
    dpd_bucket as curr_bucket
  from month_end
)
select
  month,
  prev_bucket,
  curr_bucket,
  count(*) as loans_cnt
from x
where prev_bucket is not null
group by 1,2,3
