# Pulse — Architecture Diagram

## Full Pipeline Flow
┌─────────────────────────────────────────────────────────┐
│                    DATA SOURCES                         │
│  HackerNews API    NewsAPI (RSS)    GitHub Trending API │
│  (free, no key)    (100 req/day)    (5000 req/hr)       │
└────────┬───────────────┬───────────────┬────────────────┘
│               │               │
▼               ▼               ▼
┌─────────────────────────────────────────────────────────┐
│                 INGESTION LAYER                         │
│   hn_producer.py   news_producer.py  github_producer.py│
│              run_all_producers.sh                       │
└────────────────────────┬────────────────────────────────┘
│ JSON messages
▼
┌─────────────────────────────────────────────────────────┐
│              APACHE KAFKA (Docker)                      │
│   Topic: hn-raw    Topic: news-raw   Topic: github-raw  │
│                  Kafka UI → localhost:8080               │
└────────────────────────┬────────────────────────────────┘
│ PySpark Structured Streaming
▼
┌─────────────────────────────────────────────────────────┐
│           BRONZE LAYER — Delta Lake (MinIO)             │
│   pulse-bronze/hn/    pulse-bronze/news/                │
│   pulse-bronze/github/                                  │
│              streaming/bronze_stream.py                 │
└────────────────────────┬────────────────────────────────┘
│ Airflow silver_dag (manual)
▼
┌─────────────────────────────────────────────────────────┐
│           SILVER LAYER — Delta Lake (MinIO)             │
│   Clean + Deduplicated + Unified Schema                 │
│   pulse-silver/hn/  pulse-silver/news/                  │
│   pulse-silver/github/  pulse-silver/enriched/          │
│              transforms/silver_job.py                   │
└──────────────┬──────────────────┬────────────────────────┘
│ Airflow          │ AI Enrichment DAG
│ gold_dag         ▼
│        ┌─────────────────────┐
│        │  AI ENRICHMENT      │
│        │  Gemini/OpenAI API  │
│        │  Spark UDF per row  │
│        │  sentiment + topics │
│        │  + named entities   │
│        └─────────────────────┘
│ (manual)
▼
┌─────────────────────────────────────────────────────────┐
│            GOLD LAYER — Delta Lake (MinIO)              │
│   trending_topics    language_momentum                  │
│   source_activity    top_content                        │
│   (sentiment-weighted aggregations)                     │
│              transforms/gold_job.py                     │
└──────────────┬──────────────────┬────────────────────────┘
│                  │
▼                  ▼
┌──────────────────┐   ┌──────────────────────────────────┐
│  STREAMLIT       │   │  RAG ASSISTANT                   │
│  DASHBOARD       │   │  ChromaDB (local vector store)   │
│  localhost:8501  │   │  LangChain + Gemini/OpenAI       │
│  4 pages:        │   │  Answers from YOUR pipeline data │
│  Trending Now    │   │  notebooks/rag_assistant.py      │
│  Lang Momentum   │   └──────────────────────────────────┘
│  Top Content     │
│  AI Assistant    │
└──────────────────┘
Infrastructure (Docker Compose)
┌─────────────────────────────────────────────────────────┐
│                    DOCKER SERVICES                      │
│                                                         │
│  Zookeeper     → Kafka coordinator                      │
│  Kafka         → Message broker    (port 9092)          │
│  Kafka UI      → Monitoring UI     (port 8080)          │
│  MinIO         → S3-compatible     (port 9000/9001)     │
│  Airflow       → Orchestration     (port 8081)          │
└─────────────────────────────────────────────────────────┘
Orchestration (Apache Airflow)
silver_dag       → Manual trigger → runs silver_job.py
gold_dag         → Manual trigger → runs gold_job.py
ai_enrichment    → Manual trigger → runs ai_enrichment.py

## Tech Stack
Layer               Technology
------------------------------------
Ingestion         Python, PRAW, requests
Streaming         Apache Kafka, PySpark Structured Stream
Storage           Delta Lake, MinIO (S3-compatible)
Transform         PySpark, Delta Spark
AI Enrichment     Gemini API / OpenAI, Spark UDFs
RAG               LangChain, ChromaDB, sentence-transformers
Orchestration     Apache Airflow
Dashboard         Streamlit, Plotly, Pandas
DevOps            Docker Compose, GitHub, pytest

## Data Flow Summary
APIs → Kafka → Bronze (raw) → Silver (clean)
     → AI Enrichment → Gold (aggregated)
     → ChromaDB → RAG Assistant
     → Streamlit Dashboard


## Running the Project
bash# 1. Start infrastructure
docker compose up -d

# 2. Start all producers
bash ingestion/run_all_producers.sh

# 3. Start Bronze streaming
python streaming/bronze_stream.py

# 4. Run transforms (via Airflow UI or manually)
python transforms/silver_job.py
python transforms/gold_job.py
python transforms/ai_enrichment.py

# 5. Build RAG index
python notebooks/rag_assistant.py

# 6. Launch dashboard
streamlit run dashboard/app.py
