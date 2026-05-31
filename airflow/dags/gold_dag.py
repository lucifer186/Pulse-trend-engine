"""
Gold DAG — Airflow orchestration
────────────────────────────────────────────────
Runs the Gold aggregation job every hour.
Depends on Silver DAG completing first via
ExternalTaskSensor — waits for silver run to finish
before starting Gold computation.

Open Airflow UI: http://localhost:8081
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator


default_args = {
    "owner":            "pulse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="pulse_gold_aggregation",
    description="Silver → Gold trend aggregations (hourly)",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulse", "gold", "aggregation"],
)


def run_gold(**context):
    """Run the Gold job — import and call directly."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from transforms.gold_job import run_gold_job
    run_gold_job()


def log_gold_complete(**context):
    """Log Gold completion metadata."""
    print(f"Gold aggregation complete | {context['execution_date']}")
    print("Tables updated: trending_topics, language_momentum, "
          "source_activity, top_content")


# ── Task 1: Wait for Silver to finish ────────────
# ExternalTaskSensor checks if silver DAG's
# run_silver_transform task succeeded in this same hour
t1_wait_silver = ExternalTaskSensor(
    task_id="wait_for_silver",
    external_dag_id="pulse_silver_transform",
    external_task_id="run_silver_transform",
    timeout=1800,            # wait up to 30 min for Silver
    poke_interval=60,        # check every 60 seconds
    mode="reschedule",       # free up worker slot while waiting
    dag=dag,
)

# ── Task 2: Run Gold job ─────────────────────────
# t2_gold = PythonOperator(
#     task_id="run_gold_aggregation",
#     python_callable=run_gold,
#     dag=dag,
#     execution_timeout=timedelta(minutes=30),
# )
t2_gold_bash = BashOperator(
    task_id="run_silver_transform",
    bash_command="python /opt/pulse/transforms/gold_job.py",
    env={
        # Pass all required env vars explicitly to bash
        # **os.environ,
        "PYTHONPATH": "/opt/pulse",
        "MINIO_ENDPOINT":     "http://minio:9000",
        "MINIO_ROOT_USER":    "pulseadmin",
        "MINIO_ROOT_PASSWORD":"pulsepassword123",
        "JAVA_HOME":          "/usr/lib/jvm/java-17-openjdk-amd64",
    },
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)
# ── Task 3: Log completion ───────────────────────
t3_log = PythonOperator(
    task_id="log_gold_complete",
    python_callable=log_gold_complete,
    dag=dag,
)

# ── Order: wait_silver → gold → log ─────────────
t1_wait_silver >> t2_gold_bash >> t3_log