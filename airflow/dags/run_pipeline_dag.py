from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

# Default args (optional)
default_args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id='run_pipeline_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # run manually
    catchup=False,
    tags=['pipeline'],
    default_args=default_args
)

begin = EmptyOperator(task_id="begin", dag=dag)

run_pipeline_commodity = BashOperator(
    task_id='run_pipeline_commodity',
    bash_command=(
        'python /workspaces/Exp_Imp_pipeline/run_pipeline.py '
        '--run_year {{ dag_run.conf["run_year"] if dag_run and dag_run.conf.get("run_year") else "2025" }} '
        '--category commodity'
    ),
    dag=dag
)

run_pipeline_country = BashOperator(
    task_id='run_pipeline_country',
    bash_command=(
        'python /workspaces/Exp_Imp_pipeline/run_pipeline.py '
        '--run_year {{ dag_run.conf["run_year"] if dag_run and dag_run.conf.get("run_year") else "2025" }} '
        '--category country'
    ),
    dag=dag
)

end = EmptyOperator(task_id="end", dag=dag)

begin >> run_pipeline_commodity >> run_pipeline_country >> end