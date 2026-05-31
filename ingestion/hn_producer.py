"""
HackerNews Producer
────────────────────────────────────────────────
Fetches top & new stories from the HN Firebase API
and publishes each story as a JSON message to the
Kafka topic: hn-raw

Flow:
  HN API → fetch story IDs → fetch each story → Kafka
"""

import json
import time
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from dotenv import load_dotenv
import os

# ── Load environment variables from .env ──────────
load_dotenv()

# ── Logging setup ─────────────────────────────────
# logs show timestamp + level + message in terminal
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────
KAFKA_BROKER   = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC    = "hn-raw"          # topic name in Kafka
HN_BASE_URL    = "https://hacker-news.firebaseio.com/v0"
MAX_STORIES    = 50                # how many stories to fetch per run
FETCH_INTERVAL = 300               # seconds between full runs (5 min)


# ── Kafka Producer setup ──────────────────────────
def create_producer() -> KafkaProducer:
    """
    Create and return a KafkaProducer.
    value_serializer: converts Python dict → JSON bytes automatically
    so we can send plain dicts without manual json.dumps() every time.
    """
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        # retries: if Kafka is briefly unavailable, retry 3 times
        retries=3,
        # acks='all': wait for Kafka to confirm message was written
        acks="all"
    )


# ── HackerNews API helpers ────────────────────────
def fetch_story_ids(feed: str = "top") -> list[int]:
    """
    Fetch a list of story IDs from HN.
    feed = "top" → topstories, "new" → newstories
    Returns list of integer story IDs e.g. [42001234, 42001200, ...]
    """
    url = f"{HN_BASE_URL}/{feed}stories.json"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()        # raises exception if HTTP error
        ids = resp.json()
        log.info(f"Fetched {len(ids)} {feed} story IDs from HN")
        return ids[:MAX_STORIES]       # take only the top N
    except requests.RequestException as e:
        log.error(f"Failed to fetch story IDs: {e}")
        return []


def fetch_story(story_id: int) -> dict | None:
    """
    Fetch a single story's full data by its ID.
    Returns a dict or None if the fetch fails.
    """
    url = f"{HN_BASE_URL}/item/{story_id}.json"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        log.warning(f"Failed to fetch story {story_id}: {e}")
        return None


def enrich_story(story: dict) -> dict:
    """
    Add pipeline metadata to each story before sending to Kafka.
    This is our Bronze layer enrichment — we add:
      - ingested_at: when WE fetched it (ISO format)
      - source: where it came from
      - pipeline_version: for tracking schema changes later
    """
    return {
        **story,                        # keep all original HN fields
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "hackernews",
        "pipeline_version": "1.0.0"
    }


# ── Delivery callbacks ────────────────────────────
def on_send_success(metadata):
    """Called by Kafka when message is successfully delivered."""
    log.info(
        f"✓ Sent to {metadata.topic} "
        f"[partition={metadata.partition}, offset={metadata.offset}]"
    )

def on_send_error(exc):
    """Called by Kafka if message delivery fails."""
    log.error(f"✗ Failed to send message: {exc}")


# ── Main producer loop ────────────────────────────
def run_producer():
    """
    Main loop:
    1. Connect to Kafka
    2. Fetch top + new HN story IDs
    3. For each ID: fetch story → enrich → send to Kafka
    4. Wait 5 minutes → repeat
    """
    log.info(f"Starting HN Producer → Kafka topic: {KAFKA_TOPIC}")
    log.info(f"Kafka broker: {KAFKA_BROKER}")

    producer = create_producer()
    seen_ids = set()   # track IDs we've already sent to avoid duplicates

    while True:
        log.info("─── Starting new fetch cycle ───────────────")

        # Fetch from both top and new feeds
        story_ids = []
        for feed in ["top", "new"]:
            story_ids.extend(fetch_story_ids(feed))

        # Deduplicate IDs within this batch
        story_ids = list(set(story_ids))
        new_ids   = [sid for sid in story_ids if sid not in seen_ids]
        log.info(f"New stories to process: {len(new_ids)} (skipping {len(story_ids)-len(new_ids)} already seen)")

        sent_count = 0
        for story_id in new_ids:
            story = fetch_story(story_id)

            # skip if fetch failed, or not a story type, or no title
            if not story or story.get("type") != "story" or not story.get("title"):
                continue

            enriched = enrich_story(story)

            # send to Kafka — async with callbacks
            producer.send(
                KAFKA_TOPIC,
                value=enriched,
                key=str(story_id).encode("utf-8")  # key = story ID
            ).add_callback(on_send_success).add_errback(on_send_error)

            seen_ids.add(story_id)
            sent_count += 1
            time.sleep(0.1)   # small delay to be polite to the HN API

        # flush ensures all buffered messages are sent before sleeping
        producer.flush()
        log.info(f"─── Cycle done. Sent {sent_count} stories. Sleeping {FETCH_INTERVAL}s ───")
        time.sleep(FETCH_INTERVAL)


# ── Entry point ───────────────────────────────────
if __name__ == "__main__":
    try:
        run_producer()
    except KeyboardInterrupt:
        log.info("Producer stopped by user (Ctrl+C)")
