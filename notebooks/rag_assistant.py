"""
RAG Assistant — Pulse Trend Intelligence
────────────────────────────────────────────────
Reads Gold Delta tables → indexes into ChromaDB
→ answers natural language questions using LangChain
+ OpenAI — all from YOUR pipeline data.

Usage:
  python notebooks/rag_assistant.py

Then type any question and press Enter.
Type 'quit' to exit.
"""

import os
import json
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.WARNING)   # suppress LangChain noise
log = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER     = os.getenv("MINIO_ROOT_USER",     "pulseadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "pulsepassword123")
CHROMA_DB_PATH = "./chroma_db"    # local folder for ChromaDB persistence


# ══════════════════════════════════════════════════
# STEP 1 — LOAD GOLD DATA FROM DELTA LAKE
# ══════════════════════════════════════════════════

def load_gold_data() -> list[dict]:
    """
    Read all 4 Gold Delta tables using PySpark
    and convert to list of dicts for ChromaDB indexing.

    We combine:
    - top_content: actual content items with titles
    - trending_topics: what topics are hot right now
    - language_momentum: which languages are rising
    - source_activity: which sources are most active

    Each dict becomes one ChromaDB document.
    """
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    print("Loading Gold data from Delta Lake...")

    packages = ",".join([
        "io.delta:delta-spark_2.12:3.1.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.367",
    ])

    spark = (
        SparkSession.builder
        .appName("pulse-rag-loader")
        .master("local[*]")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages",              packages)
        .config("spark.hadoop.fs.s3a.endpoint",     MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",   MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key",   MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    base     = "s3a://pulse-gold"
    documents = []

    # ── Top content → rich text documents ────────
    print("  Loading top_content...")
    top = (
        spark.read.format("delta")
        .load(f"{base}/top_content/")
        .filter("global_rank <= 100")     # top 100 items
        .collect()
    )
    for i, row in enumerate(top):
        tags_str = ", ".join(row["tags"]) if row["tags"] else ""
        doc_text = (
            f"Source: {row['source']}. "
            f"Title: {row['title']}. "
            f"Description: {row['description'] or 'N/A'}. "
            f"Author: {row['author'] or 'unknown'}. "
            f"Popularity score: {row['popularity_score']}. "
            f"Language: {row['language'] or 'N/A'}. "
            f"Tags: {tags_str}. "
            f"Published: {row['published_at']}."
        )
        documents.append({
            "id":       f"content_{i}_{row['global_rank']}",
            "text":     doc_text,
            "metadata": {
                "type":       "top_content",
                "source":     row["source"],
                "title":      row["title"][:100] if row["title"] else "",
                "score":      str(row["popularity_score"]),
                "rank":       str(row["global_rank"]),
                "language":   row["language"] or "",
            }
        })

    # ── Trending topics → topic summary documents ─
    print("  Loading trending_topics...")
    trends = (
        spark.read.format("delta")
        .load(f"{base}/trending_topics/")
        .filter("rank_in_hour <= 20")     # top 20 topics per hour
        .orderBy("hour_window", "rank_in_hour")
        .collect()
    )
    for i, row in enumerate(trends):
        titles = row["sample_titles"] if row["sample_titles"] else []
        doc_text = (
            f"Trending topic: {row['tag']}. "
            f"Time window: {row['hour_window']}. "
            f"Mentioned {row['mention_count']} times. "
            f"Across {row['source_diversity']} different sources. "
            f"Trend score: {row['trend_score']:.1f}. "
            f"Rank in this hour: {row['rank_in_hour']}. "
            f"Example titles: {'; '.join(titles[:2])}."
        )
        documents.append({
            "id":       f"trend_{i}_{row['tag']}",
            "text":     doc_text,
            "metadata": {
                "type":      "trending_topic",
                "tag":       row["tag"],
                "mentions":  str(row["mention_count"]),
                "diversity": str(row["source_diversity"]),
                "score":     str(round(row["trend_score"], 2)),
                "window":    str(row["hour_window"]),
            }
        })

    # ── Language momentum → language summary docs ─
    print("  Loading language_momentum...")
    langs = (
        spark.read.format("delta")
        .load(f"{base}/language_momentum/")
        .filter("rank_by_day <= 10")
        .orderBy("day_window", "rank_by_day")
        .collect()
    )
    for i, row in enumerate(langs):
        doc_text = (
            f"Programming language: {row['language']}. "
            f"Day: {row['day_window']}. "
            f"Momentum score: {row['momentum_score']:.0f}. "
            f"Total stars: {row['total_stars']}. "
            f"Mentioned {row['mention_count']} times. "
            f"Across {row['source_diversity']} sources. "
            f"Rank today: {row['rank_by_day']}."
        )
        documents.append({
            "id":       f"lang_{i}_{row['language']}_{row['day_window']}",
            "text":     doc_text,
            "metadata": {
                "type":     "language_momentum",
                "language": row["language"],
                "momentum": str(round(row["momentum_score"], 0)),
                "stars":    str(row["total_stars"]),
                "day":      str(row["day_window"]),
            }
        })

    spark.stop()
    print(f"  Total documents loaded: {len(documents)}")
    return documents


# ══════════════════════════════════════════════════
# STEP 2 — INDEX INTO CHROMADB
# ══════════════════════════════════════════════════

def build_vector_store(documents: list[dict]):
    """
    Convert documents to embeddings and store in ChromaDB.

    Uses sentence-transformers (all-MiniLM-L6-v2) to create
    embeddings locally — no API calls needed for indexing.
    Model downloads once (~90MB) and is cached locally.

    ChromaDB persists to disk at CHROMA_DB_PATH so
    we don't need to re-index on every run.
    """
    import chromadb
    from chromadb.utils import embedding_functions

    print("\nBuilding ChromaDB vector store...")

    # Use local sentence-transformers model for embeddings
    # This runs on your CPU — no API cost
    embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name="all-MiniLM-L6-v2"
    )

    # Persistent client — saves to disk
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)

    # Delete existing collection to rebuild fresh
    try:
        client.delete_collection("pulse_knowledge")
    except Exception:
        pass

    collection = client.create_collection(
        name="pulse_knowledge",
        embedding_function=embedding_fn,
        metadata={"hnsw:space": "cosine"}   # cosine similarity for text
    )

    # Add documents in batches (ChromaDB batch limit = 500)
    batch_size = 100
    for i in range(0, len(documents), batch_size):
        batch = documents[i:i+batch_size]
        collection.add(
            ids=[d["id"] for d in batch],
            documents=[d["text"] for d in batch],
            metadatas=[d["metadata"] for d in batch]
        )
        print(f"  Indexed batch {i//batch_size + 1}: "
              f"{len(batch)} documents")

    total = collection.count()
    print(f"ChromaDB ready — {total} documents indexed")
    return collection


# ══════════════════════════════════════════════════
# STEP 3 — RAG CHAIN WITH LANGCHAIN + GEMINI
# ══════════════════════════════════════════════════
def build_rag_chain(collection):
    from openai import OpenAI

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    MODEL  = "gpt-4.1-mini"   # cheap + fast — perfect for $5 budget [[1]]

    def ask(question: str, n_results: int = 8) -> str:
        results = collection.query(
            query_texts=[question],
            n_results=n_results,
            include=["documents","metadatas","distances"]
        )
        docs  = results["documents"][0]
        metas = results["metadatas"][0]
        dists = results["distances"][0]

        context = "\n\n---\n\n".join([
            f"[{round((1-d)*100,1)}% relevant | {m.get('type','')}]\n{doc}"
            for doc, m, d in zip(docs, metas, dists)
        ])

        prompt = f"""You are Pulse — an AI trend intelligence assistant.
        Answer ONLY using the pipeline data below. Be specific — cite numbers.
        If data doesn't contain the answer, say so clearly.

        === PULSE PIPELINE DATA ===
        {context}
        === END DATA ===

        User question: {question}
        Answer:"""

        response = client.chat.completions.create(
            model=MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=500,      # keep token usage low for $5 budget [[2]]
            temperature=0.3      # lower = more factual, less hallucination
        )
        return response.choices[0].message.content

    return ask

# ══════════════════════════════════════════════════
# MAIN — Interactive Q&A loop
# ══════════════════════════════════════════════════

def main():
    if not OPENAI_API_KEY:
        print("ERROR: OPENAI_API_KEY not set in .env")
        return

    print("\n" + "═"*55)
    print("  🚀 Pulse RAG Assistant — starting up")
    print("═"*55)

    # Load + index (takes ~2 min on first run)
    documents  = load_gold_data()
    collection = build_vector_store(documents)
    ask        = build_rag_chain(collection)

    print("\n" + "═"*55)
    print("  ✓ Ready! Ask anything about your pipeline data.")
    print("  Type 'quit' to exit.")
    print("═"*55)

    # Sample questions to try
    examples = [
        "Which programming languages are gaining momentum?",
        "What is the most popular content this week?",
        "What are developers most excited about on HackerNews?",
        "Which source is most active — HN, NewsAPI or GitHub?",
        "Which topics are people most excited about right now?",
        "Which programming language has the most negative sentiment?",
        "Is HackerNews more positive than NewsAPI today?",
        "Show me the top content with positive sentiment only",
        "Which companies are mentioned most in trending content?"
    ]
    print("\nExample questions to try:")
    for i, q in enumerate(examples, 1):
        print(f"  {i}. {q}")
    print()

    # Interactive loop
    while True:
        try:
            question = input("\n🔍 Your question: ").strip()
            if not question:
                continue
            if question.lower() in ["quit","exit","q"]:
                print("Goodbye!")
                break

            print("\n⏳ Searching your pipeline data...")
            answer = ask(question)
            print(f"\n💡 Answer:\n{answer}")
            print("\n" + "─"*55)

        except KeyboardInterrupt:
            print("\nGoodbye!")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()