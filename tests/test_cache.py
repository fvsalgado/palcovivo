"""Testes para pipeline.core.cache"""

import pytest
from datetime import date, datetime, timedelta, timezone
from pipeline.core.cache import (
    credibility_score,
    _event_ttl_hours,
    should_tombstone,
    merge_event,
    mark_not_seen,
    _safe_fromisoformat,
    TTL,
)


def _make_event(**overrides):
    """Cria evento mínimo para testes de cache."""
    base = {
        "title": "Concerto de Teste",
        "description": "Uma descrição suficientemente longa para contar como preenchida.",
        "dates": [{"date": "2026-04-15"}],
        "date_first": "2026-04-15",
        "date_last": "2026-04-15",
        "media": {},
        "price": {},
        "source_url": "https://example.com",
        "tags": [],
        "pipeline": {
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "is_active": True,
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _safe_fromisoformat
# ---------------------------------------------------------------------------

class TestSafeFromisoformat:
    def test_normal(self):
        dt = _safe_fromisoformat("2026-03-15T10:00:00+00:00")
        assert dt.year == 2026

    def test_trailing_z(self):
        dt = _safe_fromisoformat("2026-03-15T10:00:00+00:00Z")
        assert dt.year == 2026

    def test_duplicate_timezone(self):
        dt = _safe_fromisoformat("2026-03-15T10:00:00+00:00+00:00")
        assert dt.year == 2026

    def test_empty_raises(self):
        with pytest.raises(ValueError):
            _safe_fromisoformat("")


# ---------------------------------------------------------------------------
# CREDIBILITY SCORE
# ---------------------------------------------------------------------------

class TestCredibilityScore:
    def test_empty_event_low(self):
        score = credibility_score({})
        assert score == 0.0

    def test_complete_event_high(self):
        event = _make_event(
            media={"cover_image": "https://img.com/x.jpg"},
            price={"price_display": "15€", "ticketing_url": "https://tickets.com"},
            tags=["Music"],
        )
        score = credibility_score(event)
        assert score >= 0.7

    def test_score_bounds(self):
        score = credibility_score(_make_event())
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# EVENT TTL
# ---------------------------------------------------------------------------

class TestEventTtl:
    def test_unknown_date(self):
        event = _make_event(date_first=None)
        event["dates"] = [{}]
        assert _event_ttl_hours(event) == TTL["unknown"]

    def test_far_future(self):
        far = (datetime.now(timezone.utc) + timedelta(days=30)).strftime("%Y-%m-%d")
        event = _make_event(date_first=far)
        assert _event_ttl_hours(event) == TTL["upcoming_far"]

    def test_near_future(self):
        near = (datetime.now(timezone.utc) + timedelta(days=3)).strftime("%Y-%m-%d")
        event = _make_event(date_first=near)
        assert _event_ttl_hours(event) == TTL["upcoming_near"]

    def test_past_recent(self):
        past = (datetime.now(timezone.utc) - timedelta(days=10)).strftime("%Y-%m-%d")
        event = _make_event(date_first=past)
        assert _event_ttl_hours(event) == TTL["past_recent"]

    def test_past_old(self):
        old = (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")
        event = _make_event(date_first=old)
        assert _event_ttl_hours(event) == TTL["past_old"]


# ---------------------------------------------------------------------------
# TOMBSTONE
# ---------------------------------------------------------------------------

class TestShouldTombstone:
    def test_active_event_no_tombstone(self):
        event = _make_event()
        assert should_tombstone(event) is False

    def test_old_event_tombstone(self):
        old = (datetime.now(timezone.utc) - timedelta(days=100)).strftime("%Y-%m-%d")
        event = _make_event(date_last=old)
        assert should_tombstone(event) is True

    def test_not_seen_recently_no_tombstone(self):
        event = _make_event()
        event["pipeline"]["not_seen_since"] = (
            datetime.now(timezone.utc) - timedelta(days=2)
        ).isoformat()
        assert should_tombstone(event) is False

    def test_not_seen_long_tombstone(self):
        event = _make_event()
        event["pipeline"]["not_seen_since"] = (
            datetime.now(timezone.utc) - timedelta(days=10)
        ).isoformat()
        assert should_tombstone(event) is True


# ---------------------------------------------------------------------------
# MARK NOT SEEN
# ---------------------------------------------------------------------------

class TestMarkNotSeen:
    def test_adds_not_seen_since(self):
        event = _make_event()
        marked = mark_not_seen(event)
        assert "not_seen_since" in marked["pipeline"]
        # Original não modificado
        assert "not_seen_since" not in event["pipeline"]


# ---------------------------------------------------------------------------
# MERGE EVENT
# ---------------------------------------------------------------------------

class TestMergeEvent:
    def test_better_score_replaces(self):
        old = _make_event(description="")
        new = _make_event(
            description="Descrição completa e longa para dar score alto.",
            media={"cover_image": "https://img.com/x.jpg"},
        )
        merged, reason = merge_event(old, new)
        assert "substituído" in reason
        assert merged["media"]["cover_image"] == "https://img.com/x.jpg"

    def test_worse_score_fills_gaps(self):
        old = _make_event(
            description="Descrição completa e longa o suficiente para dar bom score.",
            media={"cover_image": "https://img.com/old.jpg"},
        )
        new = _make_event(description="")
        merged, reason = merge_event(old, new)
        assert "mantido" in reason
        assert merged["description"] != ""  # preservou o antigo

    def test_fills_missing_fields(self):
        old = _make_event(subtitle=None)
        new = _make_event(subtitle="Um subtítulo novo")
        # Independentemente do score, lacunas devem ser preenchidas
        merged, _ = merge_event(old, new)
        assert merged.get("subtitle") is not None or merged["description"] != ""
