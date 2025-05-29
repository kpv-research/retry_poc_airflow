from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def recovery_task(context):
    task_inst = context['task_instance']
    print(f"task {task_inst.task_id } failed in dag { task_inst.dag_id } ")
    recovery_task = TriggerDagRunOperator(
        task_id="task_rerun",
        trigger_dag_id="dag_to_call_rerun",
        conf={"message": "Task failed, triggering recovery workflow"},
        dag=dag)

default_args = {
    'owner': 'kpv',
    'depends_on_past': False,
    'email': ['kpv@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback':recovery_task
}

with DAG(
    'retry_poc',
    default_args=default_args,
    description='clear_task_instances poc',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    t2 = BashOperator(
        task_id='sleep',
        depends_on_past=False,
        bash_command='sleep 100',
    )
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """
    )

    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'my_param': 'Parameter I passed in'},
    )
    t1 >> [t2, t3]
