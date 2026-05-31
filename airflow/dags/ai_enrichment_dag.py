"""
AI Enrichment DAG
Runs every 6 hours — enriches new Silver rows with Gemini.
Runs AFTER Gold DAG completes (ExternalTaskSensor).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    "owner":            "pulse",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
    "email_on_failure": False,
}

dag = DAG(
    dag_id="pulse_ai_enrichment",
    description="Enrich Silver with Gemini sentiment + topics (6-hourly)",
    default_args=default_args,
    schedule_interval="0 */6 * * *",   # every 6 hours
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["pulse", "ai", "gemini", "enrichment"],
)


def run_enrichment(**context):
    import sys
    sys.path.insert(0, "/opt/airflow")
    from transforms.ai_enrichment import run_enrichment_job
    # Enrich 100 rows per run — fits free Gemini tier
    run_enrichment_job(sample_size=100)


def rebuild_rag_index(**context):
    """Rebuild ChromaDB index after new enrichments."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from notebooks.rag_assistant import load_gold_data, build_vector_store
    documents  = load_gold_data()
    build_vector_store(documents)
    print(f"RAG index rebuilt with {len(documents)} documents")


# ── Tasks ─────────────────────────────────────────
t1_wait_gold = ExternalTaskSensor(
    task_id="wait_for_gold",
    external_dag_id="pulse_gold_aggregation",
    external_task_id="run_gold_aggregation",
    timeout=3600,
    poke_interval=120,
    mode="reschedule",
    dag=dag,
)

t2_enrich = PythonOperator(
    task_id="run_ai_enrichment",
    python_callable=run_enrichment,
    dag=dag,
    execution_timeout=timedelta(hours=1),
)

t3_rebuild_rag = PythonOperator(
    task_id="rebuild_rag_index",
    python_callable=rebuild_rag_index,
    dag=dag,
    execution_timeout=timedelta(minutes=30),
)

# wait_gold → ai_enrichment → rebuild_rag
t1_wait_gold >> t2_enrich >> t3_rebuild_rag