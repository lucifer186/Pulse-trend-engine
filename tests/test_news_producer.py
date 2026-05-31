"""
Unit tests for NewsAPI Producer
All HTTP calls are mocked — no API key or Kafka needed.
Run: python -m pytest tests/test_news_producer.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from ingestion.news_producer import (
    fetch_top_headlines,
    fetch_by_query,
    enrich_article,
    is_valid,
    make_article_id
)


class TestMakeArticleId:

    def test_returns_32_char_hex_string(self):
        """MD5 hash should be 32 hex characters."""
        article = {"url": "https://example.com/article"}
        aid = make_article_id(article)
        assert len(aid) == 32
        assert all(c in "0123456789abcdef" for c in aid)

    def test_same_url_same_id(self):
        """Same URL must always produce same ID — deterministic."""
        article = {"url": "https://example.com/test"}
        assert make_article_id(article) == make_article_id(article)

    def test_different_urls_different_ids(self):
        """Different URLs must produce different IDs."""
        a1 = {"url": "https://example.com/article1"}
        a2 = {"url": "https://example.com/article2"}
        assert make_article_id(a1) != make_article_id(a2)

    def test_handles_missing_url(self):
        """Should not crash when URL is missing."""
        article = {}
        aid = make_article_id(article)
        assert isinstance(aid, str)
        assert len(aid) == 32


class TestFetchTopHeadlines:

    @patch("ingestion.news_producer.requests.get")
    def test_returns_list_of_articles(self, mock_get):
        """Should return list of article dicts."""
        mock_get.return_value.json.return_value = {
            "articles": [
                {"title": "AI News", "url": "https://example.com/1",
                 "source": {"name": "TechCrunch", "id": "techcrunch"}},
                {"title": "Cloud Update", "url": "https://example.com/2",
                 "source": {"name": "Wired", "id": "wired"}},
            ]
        }
        mock_get.return_value.raise_for_status = MagicMock()

        # Temporarily set env var for the test
        import os
        os.environ["NEWS_API_KEY"] = "test_key_123"
        from importlib import reload
        import ingestion.news_producer as np_mod
        reload(np_mod)

        result = np_mod.fetch_top_headlines()
        assert isinstance(result, list)

    @patch("ingestion.news_producer.requests.get")
    def test_returns_empty_list_on_failure(self, mock_get):
        """Should return empty list gracefully on API error."""
        import requests
        mock_get.side_effect = requests.RequestException("connection error")

        import os
        os.environ["NEWS_API_KEY"] = "test_key_123"
        result = fetch_top_headlines()
        assert result == []

    def test_returns_empty_without_api_key(self):
        """Should return empty list when NEWS_API_KEY not set."""
        import ingestion.news_producer as np_mod
        original = np_mod.NEWS_API_KEY   # save original
        try:
            np_mod.NEWS_API_KEY = None   # patch directly
            result = np_mod.fetch_top_headlines()
            assert result == []
        finally:
            np_mod.NEWS_API_KEY = original  # always restore


class TestFetchByQuery:

    @patch("ingestion.news_producer.requests.get")
    def test_returns_articles_for_query(self, mock_get):
        """Should return articles matching the query."""
        mock_get.return_value.json.return_value = {
            "articles": [
                {"title": "Python trending",
                 "url": "https://example.com/python",
                 "source": {"name": "Dev", "id": "dev"}}
            ]
        }
        mock_get.return_value.raise_for_status = MagicMock()

        import os
        os.environ["NEWS_API_KEY"] = "test_key_123"
        result = fetch_by_query("python")
        assert isinstance(result, list)

    @patch("ingestion.news_producer.requests.get")
    def test_handles_empty_results(self, mock_get):
        """Should handle empty articles array from API."""
        mock_get.return_value.json.return_value = {"articles": []}
        mock_get.return_value.raise_for_status = MagicMock()

        import os
        os.environ["NEWS_API_KEY"] = "test_key_123"
        result = fetch_by_query("nonexistent_topic_xyz")
        assert result == []


class TestIsValid:

    def test_removed_article_is_invalid(self):
        """NewsAPI [Removed] placeholder must be filtered out."""
        article = {
            "title": "[Removed]",
            "url":   "https://removed.com"
        }
        assert is_valid(article) is False


class TestEnrichArticle:

    def test_enriched_article_has_required_fields(self):
        """All pipeline metadata fields must be present."""
        raw = {
            "title":       "OpenAI releases new model",
            "description": "A powerful new LLM.",
            "url":         "https://example.com/openai",
            "publishedAt": "2024-01-15T10:00:00Z",
            "author":      "Jane Doe",
            "source":      {"name": "TechCrunch", "id": "techcrunch"},
            "content":     "Full article text here."
        }
        enriched = enrich_article(raw, query="artificial intelligence")

        assert "article_id"      in enriched
        assert "title"           in enriched
        assert "source_name"     in enriched
        assert "ingested_at"     in enriched
        assert "source"          in enriched
        assert "pipeline_version" in enriched
        assert enriched["source"]     == "newsapi"
        assert enriched["query_tag"]  == "artificial intelligence"
        assert enriched["source_name"] == "TechCrunch"

    def test_article_id_is_md5_hash(self):
        """article_id must be a 32-char hex string."""
        raw = {
            "title": "Test Article",
            "url":   "https://example.com/test",
            "source": {"name": "Test", "id": "test"}
        }
        enriched = enrich_article(raw)
        assert len(enriched["article_id"]) == 32

    def test_handles_missing_optional_fields(self):
        """Should not crash when optional fields are None or missing."""
        raw = {
            "title":  "Minimal article",
            "url":    "https://example.com/minimal",
            "source": {"name": "TestSource", "id": "test"}
        }
        enriched = enrich_article(raw)
        assert enriched["author"]      == ""
        assert enriched["description"] == ""

    def test_query_tag_is_none_without_query(self):
        """query_tag should be None when no query provided."""
        raw = {
            "title":  "Article without query",
            "url":    "https://example.com/nq",
            "source": {"name": "X", "id": "x"}
        }
        enriched = enrich_article(raw, query=None)
        assert enriched["query_tag"] is None