from airflow.utils.db import create_session
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.models.taskinstance import clear_task_instances
from airflow.operators.python import PythonOperator
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState

def rerun_dag_task(dag_name, task_name):
    dag_runs = DagRun.find(dag_id=dag_name)
    dag_runs_sorted = sorted(dag_runs, key=lambda dr: dr.execution_date, reverse=True)
    dag_run = dag_runs_sorted[0]
    task_run = dag_run.get_task_instance(task_id=task_name)
    with create_session() as session:
        clear_task_instances(tis=[task_run], session=session, dag=dag, dag_run_state='queued')


default_args = {
    'owner': 'Airflow',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'schedule_interval': None
}

dag = DAG(
    dag_id="dag_to_call_rerun",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

START = BashOperator(task_id="start", do_xcom_push=False, bash_command='echo "starting rerun"', dag=dag)
STOP = BashOperator(task_id="stop", do_xcom_push=False, bash_command='echo "stopping rerun"', dag=dag)

RERUN_TASK = PythonOperator(
    task_id="rerun_dag_task",
    provide_context=True,
    python_callable=rerun_dag_task,
    trigger_rule='all_success',
    depends_on_past=False,
    op_kwargs={'dag_name': "retry_poc", 'task_name': "sleep"},
    dag=dag
)

START >> RERUN_TASK >> STOP