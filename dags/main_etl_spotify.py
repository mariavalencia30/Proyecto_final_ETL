from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import extract_raw_to_table
from etl.transform import transform_clean_data
from etl.enrich import enrich_with_lastfm
from etl.validate import validate_enriched_data
from etl.load import finalize_curated_table
from etl.report import generate_reports

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="spotify_etl_full_pipeline",
    default_args=default_args,
    description="Pipeline ETL completo para Spotify + LastFM sobre PostgreSQL Railway",
    schedule_interval=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["spotify", "airflow", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_raw_to_table",
        python_callable=extract_raw_to_table,
    )

    transform_task = PythonOperator(
        task_id="transform_clean_data",
        python_callable=transform_clean_data,
    )

    enrich_task = PythonOperator(
        task_id="enrich_with_lastfm",
        python_callable=enrich_with_lastfm,
    )

    validate_task = PythonOperator(
        task_id="validate_enriched_data",
        python_callable=validate_enriched_data,
    )

    load_task = PythonOperator(
        task_id="load_final_table",
        python_callable=finalize_curated_table,
    )

    report_task = PythonOperator(
        task_id="generate_reports",
        python_callable=generate_reports,
    )

    # Flujo del DAG
    extract_task >> transform_task >> enrich_task >> validate_task >> load_task >> report_task
