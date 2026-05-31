"""
NewsAPI Producer — fetches tech headlines → Kafka topic: news-raw
"""
import json, time, hashlib, logging, os, requests
from datetime import datetime, timezone
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC    = "news-raw"
NEWS_API_KEY   = os.getenv("NEWS_API_KEY")
NEWS_BASE_URL  = "https://newsapi.org/v2"
FETCH_INTERVAL = 1800  # 30 minutes

TECH_QUERIES = [
    "artificial intelligence", "machine learning",
    "open source", "data engineering", "cloud computing"
]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3, acks="all"
    )

def make_article_id(article):
    return hashlib.md5(article.get("url", "").encode()).hexdigest()

def fetch_top_headlines():
    if not NEWS_API_KEY:
        log.error("NEWS_API_KEY not set"); return []
    try:
        resp = requests.get(f"{NEWS_BASE_URL}/top-headlines",
            params={"category":"technology","language":"en",
                    "pageSize":100,"apiKey":NEWS_API_KEY}, timeout=10)
        resp.raise_for_status()
        arts = resp.json().get("articles", [])
        log.info(f"Fetched {len(arts)} top headlines")
        return arts
    except requests.RequestException as e:
        log.error(f"Failed: {e}"); return []

def fetch_by_query(query):
    if not NEWS_API_KEY: return []
    try:
        resp = requests.get(f"{NEWS_BASE_URL}/everything",
            params={"q":query,"language":"en","sortBy":"publishedAt",
                    "pageSize":20,"apiKey":NEWS_API_KEY}, timeout=10)
        resp.raise_for_status()
        arts = resp.json().get("articles", [])
        log.info(f"Query '{query}': {len(arts)} articles")
        return arts
    except requests.RequestException as e:
        log.warning(f"Query failed: {e}"); return []

def enrich_article(article, query=None):
    return {
        "article_id":   make_article_id(article),
        "title":        article.get("title", ""),
        "description":  article.get("description", ""),
        "url":          article.get("url", ""),
        "published_at": article.get("publishedAt", ""),
        "author":       article.get("author", ""),
        "source_name":  article.get("source", {}).get("name", ""),
        "query_tag":    query,
        "ingested_at":  datetime.now(timezone.utc).isoformat(),
        "source":       "newsapi",
        "pipeline_version": "1.0.0"
    }

def is_valid(article):
    t = article.get("title", "")
    return t and "[Removed]" not in t and article.get("url", "")

def on_success(m): log.info(f"✓ news-raw [offset={m.offset}]")
def on_error(e):   log.error(f"✗ {e}")

def run_producer():
    log.info(f"Starting News Producer → {KAFKA_TOPIC}")
    producer = create_producer()
    seen_ids = set()
    while True:
        all_articles = [(a, "top") for a in fetch_top_headlines()]
        for q in TECH_QUERIES:
            all_articles += [(a, q) for a in fetch_by_query(q)]
            time.sleep(1)
        sent = 0
        for article, query in all_articles:
            if not is_valid(article): continue
            enriched = enrich_article(article, query)
            aid = enriched["article_id"]
            if aid in seen_ids: continue
            producer.send(KAFKA_TOPIC, value=enriched,
                key=aid.encode()).add_callback(on_success).add_errback(on_error)
            seen_ids.add(aid); sent += 1
        producer.flush()
        log.info(f"Sent {sent} articles. Sleeping {FETCH_INTERVAL}s")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try: run_producer()
    except KeyboardInterrupt: log.info("Stopped")