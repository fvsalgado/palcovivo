"""
Primeira Plateia — BaseScraper and WordPressEventsScraper base classes.

Provides shared HTTP session creation, retry logic, rate-limited requests,
and paginated fetching for scrapers to inherit from.
"""

import abc
import time
import logging
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

DEFAULT_TIMEOUT = 45
DEFAULT_RATE_DELAY = 1.5


def _build_session(
    headers: Optional[dict] = None,
    verify: bool = True,
) -> requests.Session:
    """
    Creates a requests.Session with retry logic:
      Retry(total=3, backoff_factor=2, status_forcelist=[429,500,502,503,504])
    """
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(headers or DEFAULT_HEADERS)
    session.verify = verify
    return session


class BaseScraper(abc.ABC):
    """
    Abstract base class for all scrapers.

    Subclasses must implement:
      - fetch_event_list() -> list[dict]
      - parse_event(raw: dict) -> dict

    Standard entry point:
      run(known_ids=None, rate_delay=None, scraper_flags=None) -> list[dict]
    """

    #: Override in subclasses to customise headers
    HEADERS: dict = DEFAULT_HEADERS
    #: Set to False in subclasses that need SSL verification disabled
    VERIFY_SSL: bool = True
    #: Default delay between requests (seconds)
    RATE_DELAY: float = DEFAULT_RATE_DELAY
    #: Default request timeout (seconds)
    TIMEOUT: int = DEFAULT_TIMEOUT

    def __init__(self) -> None:
        self.session: requests.Session = _build_session(
            headers=self.HEADERS,
            verify=self.VERIFY_SSL,
        )
        self._rate_delay: float = self.RATE_DELAY

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _get(
        self,
        url: str,
        params: Optional[dict] = None,
        headers: Optional[dict] = None,
        timeout: Optional[int] = None,
    ) -> Optional[requests.Response]:
        """Rate-limited GET.  Returns Response or None on error."""
        try:
            resp = self.session.get(
                url,
                params=params,
                headers=headers,
                timeout=timeout or self.TIMEOUT,
            )
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            logger.error(f"Timeout: GET {url}")
            return None
        except requests.exceptions.HTTPError as exc:
            logger.error(f"HTTP error: GET {url}: {exc}")
            return None
        except requests.exceptions.RequestException as exc:
            logger.error(f"Request error: GET {url}: {exc}")
            return None

    def _get_paginated(
        self,
        url: str,
        page: int,
        extra_params: Optional[dict] = None,
        page_param: str = "page",
    ) -> Optional[requests.Response]:
        """GET with a `page` query parameter merged into extra_params."""
        params = {page_param: page}
        if extra_params:
            params.update(extra_params)
        return self._get(url, params=params)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def fetch_event_list(self) -> list:
        """Retrieve the raw list of events from the source.  Returns a list of raw items."""

    @abc.abstractmethod
    def parse_event(self, raw: dict) -> dict:
        """Transform a single raw item into the normalised event dict."""

    # ------------------------------------------------------------------
    # Entry point
    # ------------------------------------------------------------------

    def run(
        self,
        known_ids=None,
        rate_delay: Optional[float] = None,
        scraper_flags: Optional[dict] = None,
    ) -> list:
        """
        Standard entry point.

        Parameters
        ----------
        known_ids:
            Collection of source_ids already cached. Subclasses may use this
            to skip already-known events.
        rate_delay:
            Override the instance-level RATE_DELAY for this run.
        scraper_flags:
            Arbitrary extra options passed through to the subclass.

        Returns
        -------
        list[dict]  Normalised event records.
        """
        if rate_delay is not None:
            self._rate_delay = rate_delay

        raw_items = self.fetch_event_list()
        return [self.parse_event(item) for item in raw_items]


# ---------------------------------------------------------------------------
# WordPressEventsScraper — for venues using WP Events Calendar REST API
# ---------------------------------------------------------------------------

class WordPressEventsScraper(BaseScraper):
    """
    Base class for scrapers that consume the WP Events Calendar REST API:
      GET /wp-json/tribe/events/v1/events?page=N&per_page=N&...

    Subclasses must set:
      API_BASE : str   – full URL of the events endpoint

    Subclasses must implement:
      parse_event(raw: dict) -> dict
    """

    API_BASE: str = ""
    PER_PAGE: int = 50
    MAX_PAGES: int = 20

    def fetch_event_list(self) -> list:
        """
        Auto-paginates the WP Events Calendar REST endpoint.
        Returns the full flat list of raw event dicts.
        """
        if not self.API_BASE:
            raise NotImplementedError("WordPressEventsScraper subclass must define API_BASE")

        all_events: list = []
        page = 1

        while page <= self.MAX_PAGES:
            logger.info(f"{self.__class__.__name__}: fetching page {page}...")
            resp = self._get_paginated(
                self.API_BASE,
                page=page,
                extra_params={"per_page": self.PER_PAGE, "status": "publish"},
            )

            if resp is None:
                logger.info(f"{self.__class__.__name__}: no response at page {page}, stopping")
                break

            try:
                data = resp.json()
            except ValueError as exc:
                logger.error(f"{self.__class__.__name__}: JSON decode error page {page}: {exc}")
                break

            if not data or "events" not in data:
                logger.info(f"{self.__class__.__name__}: no events key at page {page}")
                break

            events = data.get("events", [])
            if not events:
                break

            all_events.extend(events)
            logger.info(
                f"{self.__class__.__name__}: {len(events)} events on page {page} "
                f"(total: {len(all_events)})"
            )

            total_pages = data.get("total_pages", 1)
            if page >= total_pages:
                break

            page += 1
            time.sleep(self._rate_delay)

        logger.info(f"{self.__class__.__name__}: collection complete — {len(all_events)} raw events")
        return all_events

    @abc.abstractmethod
    def parse_event(self, raw: dict) -> dict:
        """Transform a single WP Events API event dict into a normalised record."""
