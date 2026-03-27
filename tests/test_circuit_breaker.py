"""Testes unitários para pipeline/core/circuit_breaker.py"""

import json
import pytest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pipeline.core.circuit_breaker as cb_module
from pipeline.core.circuit_breaker import (
    record_success,
    record_failure,
    is_suspended,
    get_status,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def isolated_state(tmp_path, monkeypatch):
    """Cada teste usa um ficheiro de estado isolado em tmp_path."""
    state_path = tmp_path / "circuit_breaker.json"
    monkeypatch.setattr(cb_module, "_STATE_PATH", state_path)
    return state_path


# ---------------------------------------------------------------------------
# record_success
# ---------------------------------------------------------------------------

class TestRecordSuccess:
    def test_fresh_venue_is_ok(self):
        record_success("ccb")
        s = get_status("ccb")
        assert s["status"] == "ok"
        assert s["failures"] == 0

    def test_resets_open_circuit(self):
        # Abrir o circuit breaker primeiro
        for _ in range(5):
            record_failure("ccb", max_failures=5)
        assert get_status("ccb")["status"] == "open"

        record_success("ccb")
        s = get_status("ccb")
        assert s["status"] == "ok"
        assert s["failures"] == 0
        assert s["next_retry"] is None

    def test_preserves_last_failure_timestamp(self):
        record_failure("ccb")
        ts_before = get_status("ccb")["last_failure"]
        record_success("ccb")
        # last_failure é preservado para auditoria
        assert get_status("ccb")["last_failure"] == ts_before


# ---------------------------------------------------------------------------
# record_failure
# ---------------------------------------------------------------------------

class TestRecordFailure:
    def test_increments_failures(self):
        record_failure("tndm")
        assert get_status("tndm")["failures"] == 1
        record_failure("tndm")
        assert get_status("tndm")["failures"] == 2

    def test_status_stays_ok_below_threshold(self):
        for _ in range(4):
            record_failure("tndm", max_failures=5)
        s = get_status("tndm")
        assert s["status"] == "ok"
        assert s["failures"] == 4

    def test_opens_circuit_at_threshold(self):
        for _ in range(5):
            record_failure("tndm", max_failures=5)
        s = get_status("tndm")
        assert s["status"] == "open"
        assert s["next_retry"] is not None

    def test_backoff_first_level_is_1h(self):
        for _ in range(5):
            record_failure("tndm", max_failures=5)
        s = get_status("tndm")
        next_retry = datetime.fromisoformat(s["next_retry"])
        now = datetime.now(timezone.utc)
        delta_minutes = (next_retry - now).total_seconds() / 60
        # ~1h = 60 min; tolerar 2 min de desvio de execução
        assert 58 <= delta_minutes <= 62

    def test_backoff_increases_with_failures(self):
        """Falhas extra após abrir o circuit aumentam o backoff."""
        for _ in range(5):
            record_failure("tndm", max_failures=5)
        retry_first = datetime.fromisoformat(get_status("tndm")["next_retry"])

        record_failure("tndm", max_failures=5)  # 6ª falha → backoff 6h
        retry_second = datetime.fromisoformat(get_status("tndm")["next_retry"])
        assert retry_second > retry_first

    def test_backoff_saturates_at_max(self):
        """Após esgotar os níveis de backoff, satura no último (168h)."""
        # _BACKOFF_HOURS = [1, 6, 24, 72, 168] → 5 níveis
        # max_failures=5, precisa de 5+5=10 falhas para atingir o último nível
        for _ in range(10):
            record_failure("tndm", max_failures=5)
        s = get_status("tndm")
        next_retry = datetime.fromisoformat(s["next_retry"])
        now = datetime.now(timezone.utc)
        delta_hours = (next_retry - now).total_seconds() / 3600
        # Saturado em 168h; tolerar 0.1h
        assert 167.9 <= delta_hours <= 168.1

    def test_isolates_venues(self):
        record_failure("ccb", max_failures=5)
        record_failure("ccb", max_failures=5)
        assert get_status("tndm")["failures"] == 0


# ---------------------------------------------------------------------------
# is_suspended
# ---------------------------------------------------------------------------

class TestIsSuspended:
    def test_fresh_venue_not_suspended(self):
        assert is_suspended("ccb") is False

    def test_suspended_after_threshold(self):
        for _ in range(5):
            record_failure("ccb", max_failures=5)
        assert is_suspended("ccb") is True

    def test_not_suspended_before_threshold(self):
        for _ in range(4):
            record_failure("ccb", max_failures=5)
        assert is_suspended("ccb") is False

    def test_transitions_to_half_open_when_retry_elapsed(self, isolated_state):
        """Quando next_retry passou, is_suspended retorna False e transita para half-open."""
        # Escrever estado diretamente com next_retry no passado
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        state = {
            "ccb": {
                "failures": 5,
                "last_failure": past,
                "status": "open",
                "next_retry": past,
            }
        }
        isolated_state.write_text(json.dumps(state), encoding="utf-8")

        result = is_suspended("ccb")
        assert result is False
        assert get_status("ccb")["status"] == "half-open"

    def test_suspended_when_retry_in_future(self, isolated_state):
        future = (datetime.now(timezone.utc) + timedelta(hours=6)).isoformat()
        state = {
            "tndm": {
                "failures": 5,
                "last_failure": future,
                "status": "open",
                "next_retry": future,
            }
        }
        isolated_state.write_text(json.dumps(state), encoding="utf-8")
        assert is_suspended("tndm") is True


# ---------------------------------------------------------------------------
# get_status
# ---------------------------------------------------------------------------

class TestGetStatus:
    def test_unknown_venue_returns_defaults(self):
        s = get_status("venue-que-nao-existe")
        assert s["failures"] == 0
        assert s["status"] == "ok"
        assert s["next_retry"] is None

    def test_reflects_current_state(self):
        record_failure("ccb")
        record_failure("ccb")
        s = get_status("ccb")
        assert s["failures"] == 2
        assert s["last_failure"] is not None
