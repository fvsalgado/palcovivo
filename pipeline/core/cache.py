"""
Primeira Plateia — Cache
Guarda dados raw por venue para comparação com run anterior.
Evita re-processar eventos que não mudaram.
"""

import json
import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

CACHE_DIR = Path(__file__).parent.parent.parent / "data" / "cache"
CACHE_TTL_HOURS = 23  # dados com mais de 23h são considerados stale


def _cache_path(venue_id: str) -> Path:
    return CACHE_DIR / f"{venue_id}.cache.json"


def load_cache(venue_id: str) -> dict:
    """Carrega cache do venue. Retorna {} se inexistente ou expirado."""
    path = _cache_path(venue_id)
    if not path.exists():
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        cached_at_str = data.get("cached_at", "")
        if cached_at_str:
            cached_at = datetime.fromisoformat(cached_at_str.replace("Z", "+00:00"))
            age = datetime.now(cached_at.tzinfo) - cached_at
            if age > timedelta(hours=CACHE_TTL_HOURS):
                logger.info(f"Cache {venue_id}: expirado ({age})")
                return {}

        logger.info(f"Cache {venue_id}: válido, {len(data.get('events', []))} eventos")
        return data

    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Cache {venue_id}: erro ao ler — {e}")
        return {}


def save_cache(venue_id: str, events: list[dict], metadata: dict = None) -> None:
    """Guarda eventos raw no cache do venue."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _cache_path(venue_id)

    data = {
        "venue_id": venue_id,
        "cached_at": datetime.now(timezone.utc).isoformat() + "Z",
        "event_count": len(events),
        "metadata": metadata or {},
        "events": events,
    }

    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Cache {venue_id}: guardado ({len(events)} eventos)")
    except IOError as e:
        logger.error(f"Cache {venue_id}: erro ao guardar — {e}")


def is_stale(venue_id: str) -> bool:
    """Verifica se o cache do venue está expirado ou inexistente."""
    cache = load_cache(venue_id)
    return not bool(cache)


def get_cached_events(venue_id: str) -> list[dict]:
    """Retorna eventos do cache, ou lista vazia se expirado."""
    cache = load_cache(venue_id)
    return cache.get("events", [])


def clear_cache(venue_id: str = None) -> None:
    """Limpa cache de um venue específico ou de todos."""
    if venue_id:
        path = _cache_path(venue_id)
        if path.exists():
            path.unlink()
            logger.info(f"Cache {venue_id}: limpo")
    else:
        for path in CACHE_DIR.glob("*.cache.json"):
            path.unlink()
        logger.info("Cache: todos os ficheiros limpos")
