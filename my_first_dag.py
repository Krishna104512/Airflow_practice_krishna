from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dbt_run_dag',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt'],
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /path/to/your/dbt/project && dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /path/to/your/dbt/project && dbt test',
    )

    dbt_run >> dbt_test
