with m as (
  select
    date_trunc('month', as_of_date)::date as month,
    product_type,
    currency,
    sum(exposure) as total_exposure,
    sum(case when npl_flag then exposure else 0 end) as npl_exposure
  from {{ ref('fct_dpd_daily') }}
  group by 1,2,3
)
select
  month, product_type, currency,
  total_exposure,
  npl_exposure,
  case when total_exposure = 0 then null else npl_exposure / total_exposure end as npl_ratio
from m
