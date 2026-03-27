"""
Primeira Plateia — Cache v2.1
Melhorias vs v2:
  - Cache por URL individual (não só por venue monolítico)
  - ETag / Last-Modified → HTTP 304 evita re-download
  - Content hash por evento → detecta mudanças reais
  - TTL diferenciado: futuro/próximo/distante/passado
  - Tombstone: evento desaparecido ≠ apagado imediatamente
  - Tombstone imediato para eventos passados há > TOMBSTONE_PAST_DAYS (90)
  - Regressão guard: novo scrape só substitui se score ≥ anterior
  - Retenção de backups: 7 diários + 1 semanal + 1 mensal (substitui BACKUPS_MAX fixo)
"""

import hashlib
import json
import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def _safe_fromisoformat(s: str) -> datetime:
    """
    Parse ISO 8601 timestamp de forma robusta.
    Corrige timestamps legados com Z final ou timezone duplicado (+00:00+00:00).
    """
    if not s:
        raise ValueError("timestamp vazio")
    # Remover Z final (legado: "...+00:00Z")
    s = s.rstrip("Z")
    # Corrigir timezone duplicado: "...+00:00+00:00" → "...+00:00"
    s = re.sub(r'(\+\d{2}:\d{2})\+\d{2}:\d{2}$', r'\1', s)
    return datetime.fromisoformat(s)

ROOT      = Path(__file__).parent.parent.parent
CACHE_DIR = ROOT / "data" / "cache"

# TTL por estado do evento (horas)
TTL = {
    "upcoming_near":  6,    # próximas 2 semanas → verificar frequentemente
    "upcoming_far":   48,   # mais de 2 semanas → verificar a cada 2 dias
    "ongoing":        12,   # em curso → verificar diariamente
    "past_recent":    168,  # passou há < 30 dias → 1 semana
    "past_old":       720,  # passou há > 30 dias → 30 dias (quase nunca muda)
    "unknown":        24,   # sem data → 24h
    "venue":          23,   # cache monolítico de venue (legado)
}

# Eventos sem dados mínimos são substituídos sempre
MIN_CREDIBILITY = 0.25


# ---------------------------------------------------------------------------
# CREDIBILIDADE — score de 0.0 a 1.0
# ---------------------------------------------------------------------------

def credibility_score(event: dict) -> float:
    """
    Calcula score de credibilidade de um evento (0.0–1.0).
    Usado para decidir se um novo scrape substitui dados existentes.
    """
    s = 0.0
    if event.get("title") and len(event.get("title", "")) > 2:
        s += 0.20
    if event.get("description") and len(event.get("description", "")) > 30:
        s += 0.15
    dates = event.get("dates") or []
    if dates and any(d.get("date") for d in dates):
        s += 0.20
    media = event.get("media") or {}
    if media.get("cover_image"):
        s += 0.15
    price = event.get("price") or {}
    if price.get("price_display") or price.get("is_free"):
        s += 0.10
    if event.get("source_url", "").startswith("http"):
        s += 0.10
    if event.get("tags"):
        s += 0.05
    if (event.get("price") or {}).get("ticketing_url"):
        s += 0.05

    # Penalizar dados muito antigos (scraped_at)
    scraped_at = (event.get("pipeline") or {}).get("scraped_at", "")
    if scraped_at:
        try:
            age_days = (datetime.now(timezone.utc) -
                        _safe_fromisoformat(scraped_at)).days
            s -= min(0.10, age_days * 0.003)
        except Exception:
            pass

    return round(max(0.0, min(1.0, s)), 3)


def _event_ttl_hours(event: dict) -> int:
    """Determina TTL em horas com base no estado do evento."""
    now = datetime.now(timezone.utc)
    today = now.strftime("%Y-%m-%d")
    in_2w = (now + timedelta(days=14)).strftime("%Y-%m-%d")

    date_first = event.get("date_first") or (event.get("dates") or [{}])[0].get("date")
    date_close = event.get("date_close")
    is_ongoing = event.get("is_ongoing", False)

    if not date_first:
        return TTL["unknown"]

    if is_ongoing or (date_close and date_first <= today <= date_close):
        return TTL["ongoing"]
    if date_first > in_2w:
        return TTL["upcoming_far"]
    if date_first >= today:
        return TTL["upcoming_near"]

    # Passou
    try:
        past_days = (now.date() - date.fromisoformat(date_first[:10])).days
        return TTL["past_recent"] if past_days <= 30 else TTL["past_old"]
    except Exception:
        return TTL["past_recent"]


# ---------------------------------------------------------------------------
# CACHE POR URL INDIVIDUAL
# ---------------------------------------------------------------------------

def _url_cache_path(venue_id: str, url: str) -> Path:
    """Devolve o path de cache para uma URL específica."""
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:16]
    return CACHE_DIR / venue_id / f"{url_hash}.json"


def load_url_cache(venue_id: str, url: str) -> Optional[dict]:
    """
    Carrega cache para uma URL específica.
    Retorna None se inexistente, expirado ou inválido.
    """
    path = _url_cache_path(venue_id, url)
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        cached_at_str = data.get("cached_at", "")
        event = data.get("event") or {}
        ttl   = _event_ttl_hours(event)
        if cached_at_str:
            cached_at = _safe_fromisoformat(cached_at_str)
            age = datetime.now(cached_at.tzinfo) - cached_at
            if age > timedelta(hours=ttl):
                return None
        return data
    except Exception as e:
        logger.debug(f"URL cache {url}: erro ao ler — {e}")
        return None


def save_url_cache(venue_id: str, url: str, event: dict,
                   etag: str = None, last_modified: str = None) -> None:
    """Guarda evento no cache individual por URL."""
    path = _url_cache_path(venue_id, url)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "url":           url,
        "cached_at":     datetime.now(timezone.utc).isoformat(),
        "etag":          etag,
        "last_modified": last_modified,
        "content_hash":  _content_hash(event),
        "event":         event,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_url_http_headers(venue_id: str, url: str) -> dict:
    """
    Retorna headers HTTP de cache para uma URL (ETag / Last-Modified).
    Usar para fazer conditional requests → HTTP 304 Not Modified.
    """
    cached = load_url_cache(venue_id, url)
    if not cached:
        return {}
    headers = {}
    if cached.get("etag"):
        headers["If-None-Match"] = cached["etag"]
    if cached.get("last_modified"):
        headers["If-Modified-Since"] = cached["last_modified"]
    return headers


def url_cache_unchanged(venue_id: str, url: str, new_event: dict) -> bool:
    """
    Verifica se o conteúdo do evento mudou desde o último cache.
    Usa content hash para detectar mudanças reais sem re-scraping completo.
    """
    cached = load_url_cache(venue_id, url)
    if not cached:
        return False
    old_hash = cached.get("content_hash", "")
    new_hash = _content_hash(new_event)
    return old_hash == new_hash


def _content_hash(event: dict) -> str:
    """Hash dos campos relevantes de um evento (ignora timestamps de pipeline)."""
    relevant = {k: v for k, v in event.items()
                if k not in ("pipeline", "scraped_at", "_method")}
    raw = json.dumps(relevant, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(raw.encode()).hexdigest()[:20]


# ---------------------------------------------------------------------------
# MERGE COM SCORE DE CREDIBILIDADE
# ---------------------------------------------------------------------------

def merge_event(existing: dict, new: dict) -> tuple[dict, str]:
    """
    Merge inteligente: o novo só substitui se tiver score ≥ existente.
    Para campos individuais, preenche lacunas mesmo que o score seja menor.

    Retorna (evento_final, motivo).
    """
    score_new = credibility_score(new)
    score_old = credibility_score(existing)

    if score_new >= score_old:
        # Novo é melhor ou igual → substituir, mas preservar campos ricos do antigo
        merged = new.copy()
        # Preservar campos ricos do antigo se o novo não os tem
        _fill_gaps(merged, existing)
        return merged, f"substituído (score {score_old:.2f}→{score_new:.2f})"

    # Novo é pior → manter existente mas preencher lacunas com o novo
    merged = existing.copy()
    _fill_gaps(merged, new)
    return merged, f"mantido (score existente {score_old:.2f} > novo {score_new:.2f})"


def _fill_gaps(target: dict, source: dict) -> None:
    """Preenche campos vazios em target com valores de source."""
    fill_fields = [
        "description", "subtitle", "tags",
        ("media", "cover_image"), ("media", "trailer_url"),
        ("price", "price_display"), ("price", "ticketing_url"),
        ("accessibility", "has_sign_language"),
        ("accessibility", "has_audio_description"),
    ]
    for field in fill_fields:
        if isinstance(field, tuple):
            parent, child = field
            if not (target.get(parent) or {}).get(child):
                src_val = (source.get(parent) or {}).get(child)
                if src_val:
                    target.setdefault(parent, {})[child] = src_val
        else:
            if not target.get(field) and source.get(field):
                target[field] = source[field]


# ---------------------------------------------------------------------------
# TOMBSTONE — evento desaparecido ≠ apagado
# ---------------------------------------------------------------------------

TOMBSTONE_DAYS = 7       # marcar como inactivo após N dias sem ser visto
TOMBSTONE_PAST_DAYS = 90  # tombstone imediato se evento passou há mais de N dias


def mark_not_seen(event: dict) -> dict:
    """Marca evento como 'não visto' neste scrape (tombstone candidate)."""
    event = event.copy()
    pipeline = event.get("pipeline", {}).copy()
    pipeline["last_seen_at"] = pipeline.get("last_seen_at", pipeline.get("scraped_at", ""))
    pipeline["not_seen_since"] = datetime.now(timezone.utc).isoformat()
    event["pipeline"] = pipeline
    return event


def should_tombstone(event: dict) -> bool:
    """
    Verifica se evento deve ser desactivado. Duas condições independentes:
    1. Não foi visto há > TOMBSTONE_DAYS (desapareceu do feed).
    2. O evento passou há > TOMBSTONE_PAST_DAYS (90 dias) — tombstone imediato,
       independentemente de estar ou não no feed.
    """
    # Condição 2: evento muito antigo → tombstone imediato
    date_last = event.get("date_last") or event.get("date_first")
    if date_last:
        try:
            past_days = (datetime.now(timezone.utc).date() - date.fromisoformat(date_last[:10])).days
            if past_days > TOMBSTONE_PAST_DAYS:
                return True
        except Exception:
            pass

    # Condição 1: não visto há demasiado tempo
    not_seen = (event.get("pipeline") or {}).get("not_seen_since")
    if not not_seen:
        return False
    try:
        since = _safe_fromisoformat(not_seen)
        age   = datetime.now(timezone.utc) - since
        return age > timedelta(days=TOMBSTONE_DAYS)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# CACHE MONOLÍTICO DE VENUE (legado — mantido para compatibilidade)
# ---------------------------------------------------------------------------

def _venue_cache_path(venue_id: str) -> Path:
    return CACHE_DIR / f"{venue_id}.cache.json"


def load_cache(venue_id: str) -> dict:
    """Carrega cache monolítico do venue. Retorna {} se inexistente ou expirado."""
    path = _venue_cache_path(venue_id)
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        cached_at_str = data.get("cached_at", "")
        if cached_at_str:
            cached_at = _safe_fromisoformat(cached_at_str)
            age = datetime.now(cached_at.tzinfo) - cached_at
            if age > timedelta(hours=TTL["venue"]):
                logger.info(f"Cache {venue_id}: expirado ({age})")
                return {}
        logger.info(f"Cache {venue_id}: válido, {len(data.get('events', []))} eventos")
        return data
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        logger.warning(f"Cache {venue_id}: erro ao ler — {e}")
        return {}


def save_cache(venue_id: str, events: list, metadata: dict = None) -> None:
    """Guarda eventos raw no cache monolítico do venue."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = _venue_cache_path(venue_id)
    data = {
        "venue_id":    venue_id,
        "cached_at":   datetime.now(timezone.utc).isoformat(),
        "event_count": len(events),
        "metadata":    metadata or {},
        "events":      events,
    }
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Cache {venue_id}: guardado ({len(events)} eventos)")
    except IOError as e:
        logger.error(f"Cache {venue_id}: erro ao guardar — {e}")


def is_stale(venue_id: str) -> bool:
    return not bool(load_cache(venue_id))


def get_cached_events(venue_id: str) -> list:
    return load_cache(venue_id).get("events", [])


def clear_cache(venue_id: str = None) -> None:
    if venue_id:
        # Limpar cache monolítico
        p = _venue_cache_path(venue_id)
        if p.exists():
            p.unlink()
        # Limpar cache por URL
        url_dir = CACHE_DIR / venue_id
        if url_dir.exists():
            for f in url_dir.glob("*.json"):
                f.unlink()
            url_dir.rmdir()
        logger.info(f"Cache {venue_id}: limpo")
    else:
        for path in CACHE_DIR.glob("*.cache.json"):
            path.unlink()
        for d in CACHE_DIR.iterdir():
            if d.is_dir():
                for f in d.glob("*.json"):
                    f.unlink()
                try:
                    d.rmdir()
                except OSError:
                    pass
        logger.info("Cache: todos os ficheiros limpos")


# ---------------------------------------------------------------------------
# RETENÇÃO DE BACKUPS — 7 diários + 1 semanal + 1 mensal
# ---------------------------------------------------------------------------
# Política:
#   - Manter os últimos 7 backups diários (1 por dia, o mais recente de cada dia)
#   - Manter 1 backup semanal por semana ISO nas últimas 4 semanas
#   - Manter 1 backup mensal por mês nos últimos 12 meses
#   - Apagar todos os restantes
#
# Formato dos nomes: YYYYMMDD-HHMMSS.json (já usado pelo run_venue.py)
# ---------------------------------------------------------------------------

def prune_backups(backup_dir: Path, dry_run: bool = False) -> dict:
    """
    Aplica a política de retenção a um directório de backups de venue.

    Parâmetros:
        backup_dir: Path para data/backups/{venue_id}/
        dry_run:    Se True, reporta o que faria sem apagar nada.

    Retorna:
        {"kept": int, "deleted": int, "kept_files": [...], "deleted_files": [...]}
    """
    from datetime import date as _date

    files = sorted(backup_dir.glob("*.json"))
    if not files:
        return {"kept": 0, "deleted": 0, "kept_files": [], "deleted_files": []}

    # Parsear datas dos nomes de ficheiro
    parsed: list[tuple[datetime, Path]] = []
    for f in files:
        try:
            # Formato: YYYYMMDD-HHMMSS.json
            stem = f.stem  # e.g. "20260326-160541"
            dt = datetime.strptime(stem, "%Y%m%d-%H%M%S")
            parsed.append((dt, f))
        except ValueError:
            # Nome não reconhecido → preservar por precaução
            parsed.append((datetime.min, f))

    parsed.sort(key=lambda x: x[0])
    now = datetime.now()

    # ── Seleccionar ficheiros a manter ──────────────────────────────────────

    keep: set[Path] = set()

    # 1. 7 diários: 1 por dia (o mais recente de cada dia), últimos 7 dias
    days_seen: dict[_date, Path] = {}
    for dt, f in reversed(parsed):  # mais recente primeiro
        d = dt.date()
        if d not in days_seen:
            days_seen[d] = f
    for day, f in sorted(days_seen.items(), reverse=True)[:7]:
        keep.add(f)

    # 2. 1 semanal por semana ISO, últimas 4 semanas
    weeks_seen: dict[tuple, Path] = {}
    cutoff_weekly = now - timedelta(weeks=4)
    for dt, f in reversed(parsed):
        if dt < cutoff_weekly:
            continue
        iso_week = (dt.year, dt.isocalendar()[1])  # (ano, semana ISO)
        if iso_week not in weeks_seen:
            weeks_seen[iso_week] = f
    keep.update(weeks_seen.values())

    # 3. 1 mensal por mês, últimos 12 meses
    months_seen: dict[tuple, Path] = {}
    cutoff_monthly = now - timedelta(days=365)
    for dt, f in reversed(parsed):
        if dt < cutoff_monthly:
            continue
        month_key = (dt.year, dt.month)
        if month_key not in months_seen:
            months_seen[month_key] = f
    keep.update(months_seen.values())

    # ── Apagar o que não é para manter ────────────────────────────────────
    to_delete = [f for _, f in parsed if f not in keep]

    if not dry_run:
        for f in to_delete:
            try:
                f.unlink()
            except OSError as e:
                logger.warning(f"prune_backups: não foi possível apagar {f.name} — {e}")

    kept_names    = sorted(f.name for f in keep)
    deleted_names = sorted(f.name for f in to_delete)

    logger.info(
        f"prune_backups {backup_dir.name}: "
        f"mantidos {len(kept_names)}, apagados {len(deleted_names)}"
        + (" [dry-run]" if dry_run else "")
    )

    return {
        "kept":          len(kept_names),
        "deleted":       len(deleted_names),
        "kept_files":    kept_names,
        "deleted_files": deleted_names,
    }
