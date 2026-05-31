"""
Unit tests for GitHub Producer
All HTTP calls are mocked — no internet or Kafka needed.
Run: python -m pytest tests/test_github_producer.py -v
"""
import pytest
from unittest.mock import patch, MagicMock
from ingestion.github_producer import (
    fetch_trending_repos,
    enrich_repo,
    get_headers
)


class TestGetHeaders:

    def test_returns_dict_with_accept_header(self):
        """Headers must always contain Accept field."""
        headers = get_headers()
        assert "Accept" in headers
        assert headers["Accept"] == "application/vnd.github+json"

    def test_no_auth_header_without_token(self):
        """Without GITHUB_TOKEN — no Authorization header."""
        import ingestion.github_producer as gp
        original = gp.GITHUB_TOKEN   # save original
        try:
            gp.GITHUB_TOKEN = None   # patch directly
            headers = gp.get_headers()
            assert "Authorization" not in headers
        finally:
            gp.GITHUB_TOKEN = original  # always restore

class TestFetchTrendingRepos:

    @patch("ingestion.github_producer.requests.get")
    def test_returns_list_of_repos(self, mock_get):
        """Should return a list of repo dicts."""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "items": [
                {"id": 1, "full_name": "owner/repo1",
                 "stargazers_count": 500},
                {"id": 2, "full_name": "owner/repo2",
                 "stargazers_count": 300},
            ]
        }
        mock_get.return_value.raise_for_status = MagicMock()

        result = fetch_trending_repos(language="python", days=7)
        assert isinstance(result, list)
        assert len(result) == 2

    @patch("ingestion.github_producer.requests.get")
    def test_returns_empty_on_api_failure(self, mock_get):
        """Should return empty list gracefully on error."""
        import requests
        mock_get.side_effect = requests.RequestException("timeout")
        result = fetch_trending_repos(language="python")
        assert result == []

    @patch("ingestion.github_producer.requests.get")
    def test_handles_rate_limit_403(self, mock_get):
        """Should return empty list on 403 rate limit."""
        mock_get.return_value.status_code = 403
        mock_get.return_value.headers = {"X-RateLimit-Reset": "0"}
        result = fetch_trending_repos(language="python")
        assert result == []

    @patch("ingestion.github_producer.requests.get")
    def test_returns_max_30_repos(self, mock_get):
        """Should respect per_page=30 limit."""
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = {
            "items": [{"id": i, "full_name": f"o/r{i}",
                       "stargazers_count": 100}
                      for i in range(30)]
        }
        mock_get.return_value.raise_for_status = MagicMock()
        result = fetch_trending_repos()
        assert len(result) <= 30


class TestEnrichRepo:

    def test_enriched_repo_has_required_fields(self):
        """Enriched repo must have all pipeline metadata fields."""
        raw = {
            "id": 123,
            "full_name": "owner/testrepo",
            "html_url": "https://github.com/owner/testrepo",
            "description": "A test repo",
            "stargazers_count": 500,
            "forks_count": 50,
            "language": "Python",
            "topics": ["machine-learning", "python"],
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-15T00:00:00Z",
            "owner": {"login": "owner", "type": "User"},
        }
        enriched = enrich_repo(raw, language_filter="python")

        assert enriched["repo_id"]  == 123
        assert enriched["repo_name"] == "owner/testrepo"
        assert enriched["stars"]     == 500
        assert enriched["language"]  == "Python"
        assert enriched["source"]    == "github"
        assert enriched["language_filter"] == "python"
        assert "ingested_at"       in enriched
        assert "pipeline_version"  in enriched

    def test_handles_missing_optional_fields(self):
        """Should not crash when optional fields are missing."""
        raw = {
            "id": 456,
            "full_name": "owner/minimal",
            "stargazers_count": 10,
        }
        enriched = enrich_repo(raw)
        assert enriched["repo_id"]    == 456
        assert enriched["description"] == ""
        assert enriched["topics"]      == []

    def test_preserves_topics_as_list(self):
        """Topics must remain a list — not converted to string."""
        raw = {
            "id": 789,
            "full_name": "o/r",
            "stargazers_count": 100,
            "topics": ["rust", "systems", "embedded"],
        }
        enriched = enrich_repo(raw)
        assert isinstance(enriched["topics"], list)
        assert "rust" in enriched["topics"]

    def test_pipeline_version_is_set(self):
        """pipeline_version must be present for schema tracking."""
        enriched = enrich_repo({"id": 1, "full_name": "o/r",
                                 "stargazers_count": 0})
        assert enriched["pipeline_version"] == "1.0.0"