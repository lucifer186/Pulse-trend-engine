"""
Silver DAG — fixed version
Removes PySpark health check (not available in Airflow container)
Replaces with simple MinIO/S3 file existence check instead
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import os

default_args = {
    "owner":            "pulse",
    "depends_on_past":  False,
    "retries":          1,                    # reduced from 2
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="pulse_silver_transform",
    description="Bronze → Silver transform for HN, News, GitHub",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulse", "silver", "delta-lake"],
)


def check_bronze_data(**context):
    """
    Lightweight Bronze check — NO PySpark needed.
    Uses s3fs (already installed) to check MinIO directly.
    """
    import s3fs
    import os

    fs = s3fs.S3FileSystem(
        anon=False,
        key=os.getenv("MINIO_ROOT_USER", "pulseadmin"),
        secret=os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123"),
        client_kwargs={
            "endpoint_url": os.getenv(
                "MINIO_ENDPOINT", "http://minio:9000"
            )
        }
    )

    sources = ["hn", "news", "github"]
    for source in sources:
        path = f"pulse-bronze/{source}/"
        files = fs.glob(f"{path}**/*.parquet")
        if files:
            print(f"✓ Bronze {source}: {len(files)} parquet files found")
        else:
            print(f"⚠ Bronze {source}: no data yet — skipping")

    print("Bronze health check passed")

t2_silver_bash = BashOperator(
    task_id="run_silver_transform",
    bash_command="python /opt/pulse/transforms/silver_job.py",
    env={
    #     # Pass all required env vars explicitly to bash
        # **os.environ,
        "PYTHONPATH": "/opt/pulse",
        "MINIO_ENDPOINT":     "http://minio:9000",
        "MINIO_ROOT_USER":    "pulseadmin",
        "MINIO_ROOT_PASSWORD":"pulsepassword123",
    #     "KAFKA_BROKER":       "kafka:29092",
        "JAVA_HOME":          "/usr/lib/jvm/java-17-openjdk-amd64",
    #     "PATH":               "/opt/pulse/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    },
    execution_timeout=timedelta(minutes=60),
    dag=dag,
)

def log_completion(**context):
    run_id  = context["run_id"]
    exec_dt = context["execution_date"]
    print(f"Silver complete | run_id={run_id} | exec={exec_dt}")


t1_check = PythonOperator(
    task_id="check_bronze_ready",
    python_callable=check_bronze_data,
    dag=dag,
    execution_timeout=timedelta(minutes=2),  # fast check — fail quickly
)


t3_log = PythonOperator(
    task_id="log_completion",
    python_callable=log_completion,
    dag=dag,
)

t1_check >> t2_silver_bash >> t3_log