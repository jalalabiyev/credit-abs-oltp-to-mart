from __future__ import annotations

from datetime import date, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from consumer.generators.pg_oltp_synth import run_pg_credit_oltp_synth, OLTPSynthConfig


POSTGRES_CONN_ID = "postgres_aws"   # Airflow Connection id (Postgres)
SCHEMA = "credit_oltp"


def _run():
    cfg = OLTPSynthConfig(
        schema=SCHEMA,

        # volumes (change as you want)
        n_borrowers=2000,
        n_applications=3000,
        n_loans=1500,

        # date logic
        start_date_min=date(2015, 1, 1),
        start_date_max=date.today(),

        # make ids >= 5 digits (best-effort sequence restart)
        min_borrower_id=10000,
        min_application_id=100000000,

        # snapshots (keep manageable)
        build_daily_snapshots=True,
        snapshot_days_per_loan=180,
    )
    return run_pg_credit_oltp_synth(postgres_conn_id=POSTGRES_CONN_ID, cfg=cfg)


with DAG(
    dag_id="pg_credit_oltp_abs_synth",
    start_date=datetime(2026, 1, 1),
    schedule=None,           # manual run
    catchup=False,
    tags=["credit", "oltp", "synthetic"],
) as dag:

    seed_credit_oltp = PythonOperator(
        task_id="seed_credit_oltp",
        python_callable=_run,
    )

    seed_credit_oltp