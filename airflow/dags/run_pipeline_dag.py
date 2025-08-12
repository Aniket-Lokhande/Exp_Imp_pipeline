from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.empty import EmptyOperator

dag = DAG(
    dag_id='run_pipeline_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,  # run manually
    catchup=False,
    tags=['pipeline']    
)

begin = EmptyOperator(task_id="begin", dag=dag)

run_pipeline_commodity = BashOperator(
    task_id='run_pipeline_2025_commodity',
    bash_command='python /workspaces/Exp_Imp_pipeline/run_pipeline.py --run_year 2025 --category commodity'
)

run_pipeline_country = BashOperator(
    task_id='run_pipeline_2025_country',
    bash_command='python /workspaces/Exp_Imp_pipeline/run_pipeline.py --run_year 2025 --category country'
)

end = EmptyOperator(task_id="end", dag=dag)

(
    begin >>
    run_pipeline_commodity >>
    run_pipeline_country >>
    end
)