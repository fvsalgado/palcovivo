"""
Primeira Plateia — HTTP Cache Helper
Utilitário para scrapers usarem ETag / Last-Modified em requests HTTP.
Permite HTTP 304 Not Modified → skip de re-download e re-parse.

Uso num scraper:
    from pipeline.core.http_cache import ConditionalSession

    session = ConditionalSession(venue_id="theatro-circo")
    resp = session.get_conditional(url)
    if resp is None:
        return cached_event  # não mudou
    # parse normal de resp.text...
"""

import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

logger = logging.getLogger(__name__)

ROOT      = Path(__file__).parent.parent.parent
HTTP_CACHE_DIR = ROOT / "data" / "cache" / "_http"

# Máximo de entradas no cache HTTP (por venue)
MAX_HTTP_CACHE_ENTRIES = 5000


def _http_cache_path(venue_id: str, url: str) -> Path:
    url_hash = hashlib.sha256(url.encode()).hexdigest()[:20]
    return HTTP_CACHE_DIR / venue_id / f"{url_hash}.json"


class ConditionalSession:
    """
    Wrapper sobre requests.Session com suporte a:
    - ETag / If-None-Match
    - Last-Modified / If-Modified-Since
    - Content hash para detectar mudanças quando o servidor não suporta ETag
    """

    def __init__(self, venue_id: str, session: requests.Session = None):
        self.venue_id = venue_id
        self.session  = session or requests.Session()
        self._cache_dir = HTTP_CACHE_DIR / venue_id
        self._cache_dir.mkdir(parents=True, exist_ok=True)

    def get_conditional(
        self, url: str,
        timeout: int = 20,
        extra_headers: dict = None,
    ) -> Optional[requests.Response]:
        """
        Faz GET com headers condicionais.
        Retorna:
          - Response normal (200) se o conteúdo mudou
          - None se não mudou (304 ou content hash igual)
          - None em caso de erro (usar cache existente)
        """
        headers = self._load_headers(url)
        if extra_headers:
            headers.update(extra_headers)

        try:
            resp = self.session.get(url, headers=headers, timeout=timeout)
        except requests.exceptions.RequestException as e:
            logger.debug(f"HTTP error {url}: {e}")
            return None

        if resp.status_code == 304:
            logger.debug(f"304 Not Modified: {url}")
            return None

        if not resp.ok:
            logger.debug(f"HTTP {resp.status_code}: {url}")
            return None

        # Verificar content hash mesmo sem ETag
        new_hash = hashlib.sha256(resp.content).hexdigest()[:20]
        cached = self._load_entry(url)
        if cached and cached.get("content_hash") == new_hash:
            logger.debug(f"Content hash unchanged: {url}")
            return None

        # Guardar ETag e Last-Modified para próximas requests
        self._save_entry(url, resp, new_hash)
        return resp

    def _load_headers(self, url: str) -> dict:
        """Carrega headers condicionais do cache."""
        entry = self._load_entry(url)
        if not entry:
            return {}
        headers = {}
        if entry.get("etag"):
            headers["If-None-Match"] = entry["etag"]
        if entry.get("last_modified"):
            headers["If-Modified-Since"] = entry["last_modified"]
        return headers

    def _load_entry(self, url: str) -> Optional[dict]:
        path = _http_cache_path(self.venue_id, url)
        if not path.exists():
            return None
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _save_entry(self, url: str, resp: requests.Response, content_hash: str) -> None:
        path = _http_cache_path(self.venue_id, url)
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "url":           url,
            "cached_at":     datetime.now(timezone.utc).isoformat() + "Z",
            "etag":          resp.headers.get("ETag"),
            "last_modified": resp.headers.get("Last-Modified"),
            "content_hash":  content_hash,
        }
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(entry, f)
        except Exception as e:
            logger.debug(f"Erro ao guardar HTTP cache entry: {e}")

    def prune_old_entries(self, max_age_days: int = 90) -> int:
        """Remove entradas de cache HTTP com mais de max_age_days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
        removed = 0
        for path in self._cache_dir.glob("*.json"):
            try:
                with open(path, encoding="utf-8") as f:
                    entry = json.load(f)
                cached_at = datetime.fromisoformat(
                    re.sub(r'(\+\d{2}:\d{2})\+\d{2}:\d{2}$', r'\1',
                           entry.get("cached_at", "2000-01-01").rstrip("Z"))
                )
                if cached_at < cutoff:
                    path.unlink()
                    removed += 1
            except Exception:
                pass
        if removed:
            logger.info(f"HTTP cache {self.venue_id}: {removed} entradas antigas removidas")
        return removed
