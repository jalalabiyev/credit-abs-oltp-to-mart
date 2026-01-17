with month_end as (
  select *
  from (
    select
      loan_id, as_of_date, days_past_due, dpd_bucket,
      date_trunc('month', as_of_date)::date as month,
      row_number() over (partition by loan_id, date_trunc('month', as_of_date) order by as_of_date desc) as rn
    from {{ ref('stg_arrears_daily') }}
  ) x
  where rn = 1
),
base as (
  select
    l.loan_id,
    date_trunc('quarter', l.origination_date)::date as cohort_q,
    me.month,
    (date_part('year', age(me.month, l.origination_date))*12 + date_part('month', age(me.month, l.origination_date)))::int as mob,
    (me.days_past_due > 0) as delinquent_flag,
    (me.days_past_due > 90) as npl_flag
  from month_end me
  join {{ ref('stg_loan_contract') }} l using (loan_id)
)
select
  cohort_q,
  mob,
  count(*) as loans_cnt,
  sum(delinquent_flag::int) as delinquent_cnt,
  sum(npl_flag::int) as npl_cnt,
  sum(delinquent_flag::int)::numeric / nullif(count(*),0) as delinquent_rate,
  sum(npl_flag::int)::numeric / nullif(count(*),0) as npl_rate
from base
where mob >= 0
group by 1,2
