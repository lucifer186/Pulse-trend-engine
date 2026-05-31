# 🚀 Pulse — AI-Powered Trend Intelligence Engine

Real-time data pipeline ingesting Reddit, HackerNews, News RSS & YouTube trends — enriched with AI (LLM sentiment + NER) — served via a personal intelligence dashboard.

## 🏗 Architecture Bronze → Silver → Gold (Medallion Architecture)

## 🛠 Tech Stack - **Ingestion**: Reddit API, HackerNews API, NewsAPI, YouTube API - **Streaming**: Apache Kafka, PySpark Structured Streaming - **Storage**: Delta Lake (Medallion Architecture) - **AI Enrichment**: Gemini API, spaCy NER - **Orchestration**: Apache Airflow - **Serving**: FastAPI, Streamlit, LangChain RAG - **DevOps**: Docker, Terraform, GitHub Actions

## 📁 Project Structure ``` pulse-trend-engine/ ├── ingestion/ # API producers (Reddit, HN, News, YouTube) ├── streaming/ # PySpark Structured Streaming jobs ├── transforms/ # Bronze → Silver → Gold transforms ├── airflow/dags/ # Airflow DAGs for orchestration ├── infra/ # Terraform + Docker configs ├── tests/ # Unit + integration tests ├── notebooks/ # Exploration notebooks └── docs/ # Architecture diagrams ```
## 🚦 Status: In active development