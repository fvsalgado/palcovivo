"""Testes para pipeline.core.validator"""

import pytest
from pipeline.core.validator import (
    validate_event,
    validate_and_annotate,
    validate_batch,
    quality_score,
)


def _make_event(**overrides):
    """Cria evento mínimo válido para testes."""
    base = {
        "id": "test-2026-03-concerto",
        "source_id": "test-123",
        "source_url": "https://example.com/event",
        "venue_id": "ccb",
        "title": "Concerto de Teste",
        "domain": "musica",
        "category": "musica-classica",
        "dates": [{"date": "2026-04-15", "time_start": "20:00"}],
        "pipeline": {"is_active": True},
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# VALIDATE EVENT
# ---------------------------------------------------------------------------

class TestValidateEvent:
    def test_valid_minimal(self):
        is_valid, errors, warnings = validate_event(_make_event())
        assert is_valid is True
        assert errors == []

    def test_missing_title(self):
        is_valid, errors, _ = validate_event(_make_event(title=""))
        assert is_valid is False
        assert any("titulo" in e for e in errors)

    def test_missing_dates(self):
        is_valid, errors, _ = validate_event(_make_event(dates=[]))
        assert is_valid is False
        assert any("datas" in e for e in errors)

    def test_invalid_domain(self):
        is_valid, errors, _ = validate_event(_make_event(domain="invalido"))
        assert is_valid is False
        assert any("dominio" in e for e in errors)

    def test_invalid_date_format(self):
        event = _make_event(dates=[{"date": "não-é-data", "time_start": None}])
        is_valid, errors, _ = validate_event(event)
        assert is_valid is False
        assert any("data invalida" in e for e in errors)

    def test_invalid_time_format(self):
        event = _make_event(dates=[{"date": "2026-04-15", "time_start": "8pm"}])
        is_valid, errors, _ = validate_event(event)
        assert is_valid is False
        assert any("hora invalida" in e for e in errors)

    def test_price_min_gt_max(self):
        event = _make_event(price={"price_min": 50, "price_max": 10})
        is_valid, errors, _ = validate_event(event)
        assert is_valid is False
        assert any("price_min" in e for e in errors)

    def test_invalid_source_url(self):
        event = _make_event(source_url="not-a-url")
        is_valid, errors, _ = validate_event(event)
        assert is_valid is False
        assert any("source_url" in e for e in errors)

    def test_quality_warnings_generated(self):
        """Evento válido mas com campos em falta gera warnings."""
        is_valid, errors, warnings = validate_event(_make_event())
        assert is_valid is True
        assert len(warnings) > 0  # falta descrição, imagem, preço, etc.

    def test_valid_statuses(self):
        for status in ["estreia", "reposicao", "unica-sessao", "em-cartaz"]:
            is_valid, errors, _ = validate_event(_make_event(event_status=status))
            assert is_valid is True, f"Status '{status}' deveria ser válido"

    def test_invalid_status(self):
        is_valid, errors, _ = validate_event(_make_event(event_status="inventado"))
        assert is_valid is False


# ---------------------------------------------------------------------------
# QUALITY SCORE
# ---------------------------------------------------------------------------

class TestQualityScore:
    def test_empty_event_low_score(self):
        event = _make_event()
        score = quality_score(event)
        assert 0.0 <= score <= 1.0

    def test_complete_event_high_score(self):
        event = _make_event(
            description="Uma descrição longa o suficiente para contar como preenchida neste teste.",
            media={"cover_image": "https://example.com/img.jpg"},
            price={"price_display": "15€", "is_free": False, "ticketing_url": "https://tickets.com"},
            credits={"director": "João Silva"},
            audience={"age_min": 12},
            tags=["Música", "Clássica"],
            subcategory="sinfonica",
        )
        score = quality_score(event)
        assert score >= 0.7

    def test_score_bounds(self):
        score = quality_score({})
        assert 0.0 <= score <= 1.0


# ---------------------------------------------------------------------------
# VALIDATE AND ANNOTATE
# ---------------------------------------------------------------------------

class TestValidateAndAnnotate:
    def test_annotations_added(self):
        event = validate_and_annotate(_make_event())
        assert "quality_score" in event["pipeline"]
        assert "quality_warnings" in event["pipeline"]
        assert "validated" in event["pipeline"]

    def test_valid_event_annotated(self):
        event = validate_and_annotate(_make_event())
        assert event["pipeline"]["validated"] is True

    def test_invalid_event_annotated(self):
        event = validate_and_annotate(_make_event(title=""))
        assert event["pipeline"]["validated"] is False
        assert len(event["pipeline"]["validation_errors"]) > 0


# ---------------------------------------------------------------------------
# VALIDATE BATCH
# ---------------------------------------------------------------------------

class TestValidateBatch:
    def test_mixed_batch(self):
        events = [
            _make_event(id="valid-1"),
            _make_event(id="invalid-1", title=""),
            _make_event(id="valid-2"),
        ]
        valid, invalid = validate_batch(events)
        assert len(valid) == 2
        assert len(invalid) == 1

    def test_empty_batch(self):
        valid, invalid = validate_batch([])
        assert valid == []
        assert invalid == []
