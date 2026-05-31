"""
GitHub Trending Producer — trending repos → Kafka topic: github-raw
Strategy: Search repos created in last 7 days sorted by stars = trending
"""
import json, time, logging, os, requests
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC    = "github-raw"
GITHUB_TOKEN   = os.getenv("GITHUB_TOKEN")
GITHUB_API_URL = "https://api.github.com"
FETCH_INTERVAL = 3600  # 1 hour

LANGUAGES = ["python","javascript","typescript","rust","go","java","sql"]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        retries=3, acks="all"
    )

def get_headers():
    h = {"Accept": "application/vnd.github+json",
         "X-GitHub-Api-Version": "2022-11-28"}
    if GITHUB_TOKEN:
        h["Authorization"] = f"Bearer {GITHUB_TOKEN}"
    return h

def fetch_trending_repos(language=None, days=7):
    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    query = f"created:>{since} stars:>10"
    if language: query += f" language:{language}"
    try:
        resp = requests.get(f"{GITHUB_API_URL}/search/repositories",
            headers=get_headers(),
            params={"q":query,"sort":"stars","order":"desc","per_page":30},
            timeout=10)
        if resp.status_code == 403:
            wait = max(int(resp.headers.get("X-RateLimit-Reset",0)) - int(time.time()), 60)
            log.warning(f"Rate limited — waiting {wait}s"); time.sleep(wait); return []
        resp.raise_for_status()
        items = resp.json().get("items", [])
        log.info(f"Language '{language or 'all'}': {len(items)} repos")
        return items
    except requests.RequestException as e:
        log.error(f"Failed: {e}"); return []

def enrich_repo(repo, language_filter=None):
    return {
        "repo_id":          repo.get("id"),
        "repo_name":        repo.get("full_name"),
        "repo_url":         repo.get("html_url"),
        "description":      repo.get("description", ""),
        "stars":            repo.get("stargazers_count", 0),
        "forks":            repo.get("forks_count", 0),
        "language":         repo.get("language", ""),
        "topics":           repo.get("topics", []),
        "created_at":       repo.get("created_at", ""),
        "updated_at":       repo.get("updated_at", ""),
        "owner":            repo.get("owner", {}).get("login", ""),
        "language_filter":  language_filter,
        "ingested_at":      datetime.now(timezone.utc).isoformat(),
        "source":           "github",
        "pipeline_version": "1.0.0"
    }

def on_success(m): log.info(f"✓ github-raw [offset={m.offset}]")
def on_error(e):   log.error(f"✗ {e}")

def run_producer():
    log.info(f"Starting GitHub Producer → {KAFKA_TOPIC}")
    producer = create_producer()
    seen_ids = set()
    while True:
        sent = 0
        for repo in fetch_trending_repos(language=None, days=7):
            rid = repo.get("id")
            if rid and rid not in seen_ids:
                producer.send(KAFKA_TOPIC, value=enrich_repo(repo, "all"),
                    key=str(rid).encode()).add_callback(on_success).add_errback(on_error)
                seen_ids.add(rid); sent += 1
        for lang in LANGUAGES:
            for repo in fetch_trending_repos(language=lang, days=7):
                rid = repo.get("id")
                if rid and rid not in seen_ids:
                    producer.send(KAFKA_TOPIC, value=enrich_repo(repo, lang),
                        key=str(rid).encode()).add_callback(on_success).add_errback(on_error)
                    seen_ids.add(rid); sent += 1
            time.sleep(2)
        producer.flush()
        log.info(f"Sent {sent} repos. Sleeping {FETCH_INTERVAL}s")
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    try: run_producer()
    except KeyboardInterrupt: log.info("Stopped")