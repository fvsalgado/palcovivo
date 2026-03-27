"""Testes de integração para scrapers — usa fixtures para evitar chamadas HTTP reais."""
import json
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

import requests

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_response(content, status_code=200, is_json=True):
    """Cria um mock de requests.Response."""
    resp = MagicMock(spec=requests.Response)
    resp.status_code = status_code
    resp.ok = status_code < 400
    if is_json:
        resp.json.return_value = content
        resp.text = json.dumps(content)
    else:
        resp.text = content
        resp.json.side_effect = ValueError("not JSON")
    resp.raise_for_status.return_value = None
    return resp


def _fixture_json(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# CCB tests
# ---------------------------------------------------------------------------

class TestCCBScraper:

    def test_ccb_parses_events(self):
        """run() com fixture de uma página devolve lista com evento válido."""
        from pipeline.scrapers.ccb.scraper import run

        fixture = _fixture_json("ccb_events_page1.json")
        # A API devolve a fixture na primeira chamada; páginas subsequentes devolvem {}
        side_effects = [
            _mock_response(fixture),
            _mock_response({}),
        ]

        with patch("requests.Session.get", side_effect=side_effects):
            # Desativar enriquecimento HTML para este teste (evita calls adicionais)
            with patch("pipeline.scrapers.ccb.scraper.ENRICH_DETAIL_PAGES", False):
                result = run()

        assert isinstance(result, list)
        assert len(result) >= 1

        first = result[0]
        assert "title" in first
        assert "source_id" in first
        assert "dates" in first
        assert "source_url" in first
        assert first["title"] == "Concerto de Primavera"

    def test_ccb_handles_empty_response(self):
        """run() com resposta vazia devolve []."""
        from pipeline.scrapers.ccb.scraper import run

        with patch("requests.Session.get", return_value=_mock_response({})):
            with patch("pipeline.scrapers.ccb.scraper.ENRICH_DETAIL_PAGES", False):
                result = run()

        assert result == []

    def test_ccb_handles_http_error(self):
        """run() quando o GET levanta HTTPError devolve []."""
        from pipeline.scrapers.ccb.scraper import run

        mock_resp = MagicMock(spec=requests.Response)
        mock_resp.status_code = 503
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            "503 Service Unavailable"
        )

        with patch("requests.Session.get", return_value=mock_resp):
            with patch("pipeline.scrapers.ccb.scraper.ENRICH_DETAIL_PAGES", False):
                result = run()

        assert result == []


# ---------------------------------------------------------------------------
# Culturgest tests
# ---------------------------------------------------------------------------

class TestCulturgestScraper:

    def test_culturgest_sitemap_filtering(self):
        """_is_programacao_event() aceita URLs PT correctos e rejeita os inválidos."""
        from pipeline.scrapers.culturgest.scraper import _is_programacao_event

        # URLs que devem passar
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/espectaculo-teste/"
        ) is True
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/concerto-2026/"
        ) is True

        # URLs que devem ser rejeitados
        # Versão inglesa
        assert _is_programacao_event(
            "https://www.culturgest.pt/en/programme/espectaculo-teste/"
        ) is False
        # Slug de sistema
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/por-evento/"
        ) is False
        # Outros slugs de sistema
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/agenda-pdf/"
        ) is False
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/por-semana/"
        ) is False
        # Com query string
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/evento/?ref=123"
        ) is False
        # Demasiados segmentos de path
        assert _is_programacao_event(
            "https://www.culturgest.pt/pt/programacao/evento/sub/"
        ) is False

    def test_culturgest_run_with_fixture(self):
        """
        run() com sitemap fixture real — os eventos sem detalhe (resposta None)
        são descartados, resultado final é [].
        """
        from pipeline.scrapers.culturgest.scraper import run

        sitemap_xml = _fixture_text("culturgest_sitemap.xml")
        sitemap_resp = _mock_response(sitemap_xml, is_json=False)

        # Primeira chamada → sitemap; chamadas seguintes para detail → None (falha)
        def side_effect(url, **kwargs):
            if "sitemap" in url:
                return sitemap_resp
            # Simular falha no fetch da página de detalhe
            raise requests.exceptions.ConnectionError("simulated failure")

        with patch("requests.Session.get", side_effect=side_effect):
            result = run()

        # Eventos sem datas (detalhe não carregado) são descartados
        assert result == []


# ---------------------------------------------------------------------------
# BaseScraper tests
# ---------------------------------------------------------------------------

class TestBaseScraper:

    def test_base_scraper_session(self):
        """BaseScraper inicializa com requests.Session com retry adapter."""
        from pipeline.core.base_scraper import BaseScraper
        from requests.adapters import HTTPAdapter

        # Criar subclasse concreta mínima para instanciar BaseScraper
        class _DummyScraper(BaseScraper):
            def fetch_event_list(self):
                return []

            def parse_event(self, raw):
                return raw

        scraper = _DummyScraper()

        assert isinstance(scraper.session, requests.Session)

        # Verificar que tem adapter com retry em https://
        adapter = scraper.session.get_adapter("https://example.com")
        assert isinstance(adapter, HTTPAdapter)
        assert adapter.max_retries is not None
        assert adapter.max_retries.total == 3
