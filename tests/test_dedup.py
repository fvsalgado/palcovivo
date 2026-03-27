"""Testes para pipeline.core.dedup"""

import pytest
from pipeline.core.dedup import (
    build_fingerprint_index,
    resolve_duplicates,
    merge_sessions,
    deduplicate,
)


def _make_event(venue_id="ccb", title="Concerto", date_first="2026-04-15",
                fingerprint=None, description="", cover_image=None, **extra):
    """Cria evento mínimo com dedup metadata."""
    fp = fingerprint or f"{title.lower().replace(' ', '-')}|{date_first[:7]}"
    event = {
        "id": f"{venue_id}-{date_first[:7]}-{title.lower().replace(' ', '-')}",
        "venue_id": venue_id,
        "title": title,
        "date_first": date_first,
        "description": description,
        "media": {"cover_image": cover_image},
        "dates": [{"date": date_first, "time_start": "20:00"}],
        "price": {},
        "credits": {},
        "dedup": {
            "fingerprint": fp,
            "duplicate_of": None,
            "is_canonical": True,
            "seen_at_venues": [venue_id],
        },
        "pipeline": {"is_active": True},
        "is_multi_venue": False,
        "multi_venue_ids": [],
    }
    event.update(extra)
    return event


# ---------------------------------------------------------------------------
# BUILD FINGERPRINT INDEX
# ---------------------------------------------------------------------------

class TestBuildFingerprintIndex:
    def test_groups_by_fingerprint(self):
        events = [
            _make_event(fingerprint="fp-a"),
            _make_event(fingerprint="fp-b"),
            _make_event(fingerprint="fp-a"),
        ]
        index = build_fingerprint_index(events)
        assert len(index["fp-a"]) == 2
        assert len(index["fp-b"]) == 1

    def test_empty_list(self):
        assert build_fingerprint_index([]) == {}


# ---------------------------------------------------------------------------
# RESOLVE DUPLICATES
# ---------------------------------------------------------------------------

class TestResolveDuplicates:
    def test_no_duplicates(self):
        events = [
            _make_event(fingerprint="fp-a"),
            _make_event(fingerprint="fp-b"),
        ]
        result = resolve_duplicates(events)
        assert len(result) == 2

    def test_duplicate_resolved(self):
        """O evento com mais dados fica canónico."""
        events = [
            _make_event(venue_id="ccb", fingerprint="fp-dup",
                        description=""),
            _make_event(venue_id="culturgest", fingerprint="fp-dup",
                        description="Uma descrição completa do evento",
                        cover_image="https://example.com/img.jpg"),
        ]
        result = resolve_duplicates(events)
        assert len(result) == 1
        canonical = result[0]
        assert canonical["venue_id"] == "culturgest"
        assert canonical["dedup"]["is_canonical"] is True
        assert canonical["is_multi_venue"] is True

    def test_duplicate_venues_tracked(self):
        events = [
            _make_event(venue_id="ccb", fingerprint="fp-dup"),
            _make_event(venue_id="tndm", fingerprint="fp-dup"),
        ]
        result = resolve_duplicates(events)
        canonical = result[0]
        assert set(canonical["dedup"]["seen_at_venues"]) == {"ccb", "tndm"}


# ---------------------------------------------------------------------------
# MERGE SESSIONS
# ---------------------------------------------------------------------------

class TestMergeSessions:
    def test_same_event_different_sessions_merged(self):
        """Duas sessões do mesmo evento no mesmo venue são consolidadas."""
        events = [
            _make_event(venue_id="ccb", fingerprint="ccb|fp",
                        date_first="2026-04-15"),
            _make_event(venue_id="ccb", fingerprint="ccb|fp",
                        date_first="2026-04-16"),
        ]
        # O segundo evento deve ter uma data diferente
        events[1]["dates"] = [{"date": "2026-04-16", "time_start": "20:00"}]
        result = merge_sessions(events)
        assert len(result) == 1
        assert len(result[0]["dates"]) == 2

    def test_different_events_not_merged(self):
        events = [
            _make_event(fingerprint="fp-a"),
            _make_event(fingerprint="fp-b"),
        ]
        result = merge_sessions(events)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# DEDUPLICATE (pipeline completo)
# ---------------------------------------------------------------------------

class TestDeduplicate:
    def test_full_pipeline(self):
        events = [
            _make_event(venue_id="ccb", title="Concerto", fingerprint="fp"),
            _make_event(venue_id="tndm", title="Concerto", fingerprint="fp",
                        description="Descrição completa"),
        ]
        result = deduplicate(events)
        assert len(result) == 1

    def test_no_events(self):
        assert deduplicate([]) == []
