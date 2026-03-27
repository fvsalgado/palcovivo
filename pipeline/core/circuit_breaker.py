"""Circuit breaker per venue — suspende venues com falhas consecutivas."""

import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

_logger = logging.getLogger(__name__)

# Raiz do projecto (dois níveis acima de pipeline/core/)
_ROOT = Path(__file__).parent.parent.parent
_STATE_PATH = _ROOT / "data" / "cache" / "circuit_breaker.json"

# Backoff em horas por nível de falha consecutivo (índice = failures - max_failures)
_BACKOFF_HOURS = [1, 6, 24, 72, 168]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_state() -> dict:
    """Carrega estado do circuit breaker. Retorna dict vazio se ficheiro ausente ou inválido."""
    try:
        if _STATE_PATH.exists():
            with open(_STATE_PATH, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        _logger.warning("circuit_breaker: erro ao ler estado — %s", e)
    return {}


def _save_state(state: dict) -> None:
    """Guarda estado atomicamente."""
    try:
        _STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = _STATE_PATH.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        tmp.replace(_STATE_PATH)
    except Exception as e:
        _logger.warning("circuit_breaker: erro ao guardar estado — %s", e)


def record_success(venue_id: str) -> None:
    """Regista sucesso para um venue: reset failures=0, status='ok'."""
    state = _load_state()
    entry = state.get(venue_id, {})
    if entry.get("failures", 0) > 0 or entry.get("status", "ok") != "ok":
        _logger.info("circuit_breaker: %s — reset (era %s, %d falhas)", venue_id,
                     entry.get("status", "ok"), entry.get("failures", 0))
    state[venue_id] = {
        "failures": 0,
        "last_failure": entry.get("last_failure"),
        "status": "ok",
        "next_retry": None,
    }
    _save_state(state)


def record_failure(venue_id: str, max_failures: int = 5) -> None:
    """
    Regista falha para um venue.
    Se failures >= max_failures, abre o circuit breaker com backoff.
    """
    state = _load_state()
    entry = state.get(venue_id, {"failures": 0, "last_failure": None, "status": "ok", "next_retry": None})

    failures = entry.get("failures", 0) + 1
    now_str = _now_iso()

    if failures >= max_failures:
        # Calcular nível de backoff (satura no último valor)
        backoff_index = min(failures - max_failures, len(_BACKOFF_HOURS) - 1)
        backoff_h = _BACKOFF_HOURS[backoff_index]
        next_retry = (datetime.now(timezone.utc) + timedelta(hours=backoff_h)).isoformat()
        status = "open"
        _logger.warning(
            "circuit_breaker: %s ABERTO após %d falhas consecutivas — "
            "próxima tentativa em %dh (%s)",
            venue_id, failures, backoff_h, next_retry
        )
    else:
        next_retry = entry.get("next_retry")
        status = entry.get("status", "ok")
        _logger.info("circuit_breaker: %s — %d/%d falhas consecutivas", venue_id, failures, max_failures)

    state[venue_id] = {
        "failures": failures,
        "last_failure": now_str,
        "status": status,
        "next_retry": next_retry,
    }
    _save_state(state)


def is_suspended(venue_id: str) -> bool:
    """
    Retorna True se o venue está suspenso (status=='open' e now < next_retry).
    Transita automaticamente para 'half-open' quando next_retry é atingido.
    """
    state = _load_state()
    entry = state.get(venue_id)
    if not entry:
        return False

    if entry.get("status") != "open":
        return False

    next_retry_str = entry.get("next_retry")
    if not next_retry_str:
        return False

    try:
        next_retry = datetime.fromisoformat(next_retry_str)
        now = datetime.now(timezone.utc)
        if now >= next_retry:
            # Transitar para half-open — permitir uma tentativa
            entry["status"] = "half-open"
            state[venue_id] = entry
            _save_state(state)
            _logger.info("circuit_breaker: %s — transitado para half-open", venue_id)
            return False
        return True
    except Exception as e:
        _logger.warning("circuit_breaker: erro ao avaliar next_retry para %s — %s", venue_id, e)
        return False


def get_status(venue_id: str) -> dict:
    """Retorna o estado actual do circuit breaker para o venue."""
    state = _load_state()
    return state.get(venue_id, {"failures": 0, "last_failure": None, "status": "ok", "next_retry": None})
