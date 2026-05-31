#!/bin/bash
set -e
mkdir -p logs
echo "🚀 Starting all Pulse producers..."
source venv/bin/activate
python ingestion/hn_producer.py     >> logs/hn.log     2>&1 & echo "✓ HN producer (PID: $!)"
python ingestion/news_producer.py   >> logs/news.log   2>&1 & echo "✓ News producer (PID: $!)"
python ingestion/github_producer.py >> logs/github.log 2>&1 & echo "✓ GitHub producer (PID: $!)"
echo ""
echo "Monitor: tail -f logs/hn.log | logs/news.log | logs/github.log"
echo "Stop: kill \$(pgrep -f 'hn_producer|news_producer|github_producer')"