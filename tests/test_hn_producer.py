"""
Unit tests for HN Producer
Tests the API fetch functions WITHOUT needing Kafka running.
We mock the HTTP calls so tests are fast and offline-safe.
"""

import pytest
from unittest.mock import patch, MagicMock
from ingestion.hn_producer import fetch_story_ids, fetch_story, enrich_story


class TestFetchStoryIds:
    """Tests for the fetch_story_ids function"""

    @patch("ingestion.hn_producer.requests.get")
    def test_returns_list_of_ids(self, mock_get):
        """Should return a list of integers"""
        mock_get.return_value.json.return_value = [1, 2, 3, 4, 5]
        mock_get.return_value.raise_for_status = MagicMock()

        result = fetch_story_ids("top")
        assert isinstance(result, list)
        assert len(result) <= 50    # MAX_STORIES

    @patch("ingestion.hn_producer.requests.get")
    def test_handles_api_failure_gracefully(self, mock_get):
        """Should return empty list if API is down — not crash"""
        import requests
        mock_get.side_effect = requests.RequestException("API down")

        result = fetch_story_ids("top")
        assert result == []         # graceful empty return


class TestFetchStory:
    """Tests for the fetch_story function"""

    @patch("ingestion.hn_producer.requests.get")
    def test_returns_story_dict(self, mock_get):
        """Should return a dict with story fields"""
        mock_story = {
            "id": 42000000,
            "type": "story",
            "title": "Show HN: My cool project",
            "score": 100,
            "by": "testuser"
        }
        mock_get.return_value.json.return_value = mock_story
        mock_get.return_value.raise_for_status = MagicMock()

        result = fetch_story(42000000)
        assert result["id"] == 42000000
        assert result["title"] == "Show HN: My cool project"

    @patch("ingestion.hn_producer.requests.get")
    def test_returns_none_on_failure(self, mock_get):
        """Should return None if story fetch fails"""
        import requests
        mock_get.side_effect = requests.RequestException("timeout")

        result = fetch_story(99999)
        assert result is None


class TestEnrichStory:
    """Tests for the enrich_story function"""

    def test_adds_required_metadata_fields(self):
        """Enriched story should have our pipeline metadata"""
        raw_story = {"id": 1, "title": "Test", "score": 50}
        enriched = enrich_story(raw_story)

        assert "ingested_at" in enriched
        assert enriched["source"] == "hackernews"
        assert enriched["pipeline_version"] == "1.0.0"

    def test_preserves_original_fields(self):
        """Original HN fields must not be lost during enrichment"""
        raw_story = {"id": 1, "title": "Test", "score": 50, "by": "user"}
        enriched = enrich_story(raw_story)

        assert enriched["id"] == 1
        assert enriched["title"] == "Test"
        assert enriched["score"] == 50