# credit-abs-oltp-to-mart

Synthetic credit OLTP → dbt analytics mart pipeline to compute ABS-like credit risk metrics
 (DPD / NPL / Roll-rate / Cure-rate / Vintage).

# What this project does
1) Generates synthetic data in PostgreSQL (credit_oltp schema)  
2) Builds dbt staging + marts in PostgreSQL (credit_mart schema)  
3) Exposes analytics-friendly views for reporting / BI

# Tech stack
- PostgreSQL (OLTP + Analytics schemas)
- dbt (models + tests)
- (Optional) Airflow (orchestration)

# Architecture (simple)
- Source / OLTP:credit_oltp. (loan_contract, repayment_schedule, repayment_payment, arrears_dpd_status,
    write_off_and_recovery, collections_case)
- dbt staging:credit_mart.stg (clean/standardize types & keys)
- dbt marts: credit_mart.fct (metrics & aggregates)

# Examples
- DPD buckets: 1–30, 31–60, 61–90, 90+ days past due  
- NPL ratio:npl_exposure / total_exposure
- Roll-rate: transition rate between delinquency buckets month-to-month
- Cure rate: delinquent → performing recovery rate
- Vintage / MOB: cohort delinquency performance by origination month

