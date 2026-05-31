
# 🚀 Pulse — AI-Powered Trend Intelligence Engine

> Real-time data pipeline ingesting **HackerNews**, **NewsAPI** and **GitHub Trending** — enriched with **Gemini/OpenAI AI** — served via a **live Streamlit dashboard** with a **RAG AI assistant**.

[![Python](https://img.shields.io/badge/Python-3.12-blue)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-orange)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1.0-blue)](https://delta.io)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.32-red)](https://streamlit.io)
[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-green)](https://airflow.apache.org)

---

## 📌 What is Pulse?

Pulse is a **production-grade data engineering portfolio project** that demonstrates:

- **Real-time streaming** with Apache Kafka + PySpark Structured Streaming
- **Medallion Architecture** (Bronze → Silver → Gold) using Delta Lake
- **AI enrichment** with Gemini/OpenAI inside Spark UDFs
- **RAG AI assistant** powered by LangChain + ChromaDB
- **Multi-page Streamlit dashboard** with live trend charts
- **Workflow orchestration** with Apache Airflow
- **Zero cloud cost** — runs entirely on your laptop via Docker

---

## 🏗️ Architecture
Check docs folder

## 📁 Project Structure
pulse-trend-engine/
├── ingestion/
│   ├── hn_producer.py          # HackerNews → Kafka \
│   ├── news_producer.py        # NewsAPI → Kafka \
│   ├── github_producer.py      # GitHub Trending → Kafka \
│   └── run_all_producers.sh    # Start all 3 producers \
│
├── streaming/
│   └── bronze_stream.py        # Kafka → Delta Lake Bronze \
│
├── transforms/
│   ├── silver_job.py           # Bronze → Silver (clean + unified) \
│   ├── silver_transforms.py    # Silver helper functions \
│   ├── gold_job.py             # Silver → Gold (aggregations) \
│   ├── gold_transforms.py      # Gold helper functions \
│   └── ai_enrichment.py        # Gemini UDF enrichment \
│
├── airflow/dags/
│   ├── silver_dag.py           # Manual trigger Silver DAG \
│   ├── gold_dag.py             # Manual trigger Gold DAG \
│   └── ai_enrichment_dag.py   # Manual trigger AI DAG \
│
├── dashboard/
│   ├── app.py                  # Home page \
│   ├── data_loader.py          # MinIO data reader (local) \
│   └── pages/
│       ├── 1_Trending_Now.py \
│       ├── 2_Language_Momentum.py \
│       ├── 3_Top_Content.py \
│       └── 4_AI_Assistant.py \
│
├── notebooks/
│   └── rag_assistant.py        # RAG Q&A CLI + index builder \
│
├── infra/
│   └── setup_minio.py          # Create MinIO buckets \
│
├── tests/
│   ├── test_hn_producer.py \
│   ├── test_news_producer.py \
│   ├── test_github_producer.py \
│   ├── test_silver_transforms.py \
│   └── test_gold_transforms.py \
│
├── docs/
│   └── architecture.md         # Full architecture diagram \
│
├── docker-compose.yml          # Kafka, MinIO, Airflow, Zookeeper \
├── start_pulse.sh              # One-command startup script \
├── requirements.txt \
├── .env.example \
└── README.md \

---

## ⚙️ Prerequisites

| Requirement | Version |
|---|---|
| Windows 10/11 with WSL2 (Ubuntu 24.04) | WSL2 |
| Docker Desktop | Latest |
| Python | 3.12 |
| Java JDK | 11 |
| RAM | 16 GB recommended |

---

## 🚀 Quick Start

### 1. Clone the repository
```bash
git clone git@github.com:YOUR_USERNAME/pulse-trend-engine.git
cd pulse-trend-engine
```

### 2. Create virtual environment
```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

### 3. Set up environment variables
```bash
cp .env.example .env
# Edit .env and add your API keys:
# NEWS_API_KEY    → https://newsapi.org/register
# GITHUB_TOKEN    → GitHub Settings → Developer settings → PAT
# GEMINI_API_KEY  → https://aistudio.google.com/app/apikey
# OPENAI_API_KEY  → https://platform.openai.com (optional)
```

### 4. Start everything with one command
```bash
bash start_pulse.sh
```

**Or manually step by step:**
```bash
# Terminal 1 — Infrastructure
docker compose up -d

# Terminal 2 — Start producers (after 30s)
bash ingestion/run_all_producers.sh

# Terminal 3 — Bronze streaming
python streaming/bronze_stream.py

# Terminal 4 — Run transforms
python transforms/silver_job.py
python transforms/gold_job.py
python transforms/ai_enrichment.py

# Terminal 5 — Build RAG index
python notebooks/rag_assistant.py

# Terminal 6 — Launch dashboard
streamlit run dashboard/app.py
```

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Kafka UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | pulseadmin / pulsepassword123 |
| Airflow UI | http://localhost:8081 | admin / admin |

---

## 📊 Gold Tables (pulse-gold/)

| Table | Description |
|---|---|
| `trending_topics` | Top topics by hour — sentiment-weighted trend scores |
| `language_momentum` | Programming language velocity — stars + mentions |
| `source_activity` | Publishing rate per source per hour |
| `top_content` | Global leaderboard — top 50 items per source |

---

## 🤖 AI Features

### AI Enrichment (Gemini/OpenAI inside Spark UDF)
- Runs on Silver rows via PySpark UDF
- Adds: `sentiment` (positive/negative/neutral), `ai_topics`, `ai_entities`
- Output: `pulse-silver/enriched/` Delta table
- Run: `python transforms/ai_enrichment.py`

### RAG Assistant (LangChain + ChromaDB)
- Indexes Gold tables into local ChromaDB vector store
- Answers natural language questions from YOUR pipeline data
- Supports: Gemini, OpenAI, Groq (selectable in dashboard)
- Run: `python notebooks/rag_assistant.py`

**Example questions:**
"What are the top trending topics right now?"
"Which programming languages are gaining momentum?"
"What is the most popular content this week?"
"Which topics have positive sentiment?"
