"""Testes para pipeline.core.harmonizer"""

import pytest
from pipeline.core.harmonizer import (
    slugify,
    normalize_title,
    clean_description,
    truncate_description,
    parse_date,
    parse_time,
    parse_price,
    harmonize_category,
    harmonize_audience,
    detect_event_status,
    generate_event_id,
    generate_fingerprint,
    harmonize_event,
)


# ---------------------------------------------------------------------------
# SLUGIFY
# ---------------------------------------------------------------------------

class TestSlugify:
    def test_basic(self):
        assert slugify("Hello World") == "hello-world"

    def test_accents(self):
        assert slugify("São João da Música") == "sao-joao-da-musica"

    def test_special_chars(self):
        assert slugify("Concerto #1 — Beethoven!") == "concerto-1--beethoven"

    def test_multiple_spaces(self):
        assert slugify("  muitos   espaços  ") == "muitos-espacos"

    def test_empty(self):
        assert slugify("") == ""


# ---------------------------------------------------------------------------
# NORMALIZE TITLE
# ---------------------------------------------------------------------------

class TestNormalizeTitle:
    def test_html_removal(self):
        assert normalize_title("<b>Concerto</b> de Jazz") == "Concerto de Jazz"

    def test_double_spaces(self):
        assert normalize_title("Muitos    espaços") == "Muitos espaços"

    def test_trailing_punctuation(self):
        assert normalize_title("Título com ponto.") == "Título com ponto"

    def test_empty(self):
        assert normalize_title("") == ""

    def test_none_like(self):
        assert normalize_title("") == ""

    def test_quotes_normalization(self):
        result = normalize_title('Concerto "especial"')
        assert "«" in result or '"' not in result


# ---------------------------------------------------------------------------
# CLEAN DESCRIPTION
# ---------------------------------------------------------------------------

class TestCleanDescription:
    def test_br_to_newline(self):
        assert "\n" in clean_description("Linha 1<br>Linha 2")

    def test_p_to_double_newline(self):
        result = clean_description("<p>Parágrafo 1</p><p>Parágrafo 2</p>")
        assert "Parágrafo 1" in result
        assert "Parágrafo 2" in result

    def test_strip_tags(self):
        assert "<" not in clean_description("<div class='x'>Texto</div>")

    def test_empty(self):
        assert clean_description("") == ""


# ---------------------------------------------------------------------------
# TRUNCATE DESCRIPTION
# ---------------------------------------------------------------------------

class TestTruncateDescription:
    def test_short_unchanged(self):
        assert truncate_description("Curto", 300) == "Curto"

    def test_long_truncated(self):
        text = "Palavra " * 100
        result = truncate_description(text, 50)
        assert len(result) <= 55  # 50 + "…" + margem de palavra
        assert result.endswith("…")

    def test_no_break_mid_word(self):
        result = truncate_description("Uma frase com várias palavras aqui", 20)
        assert not result.rstrip("…").endswith(" ")  # não corta a meio


# ---------------------------------------------------------------------------
# PARSE DATE
# ---------------------------------------------------------------------------

class TestParseDate:
    def test_iso_format(self):
        assert parse_date("2026-03-15") == "2026-03-15"

    def test_iso_with_time(self):
        assert parse_date("2026-03-15T20:00:00") == "2026-03-15"

    def test_dd_mm_yyyy_slash(self):
        assert parse_date("15/03/2026") == "2026-03-15"

    def test_dd_mm_yyyy_dash(self):
        assert parse_date("15-03-2026") == "2026-03-15"

    def test_dd_mm_yy(self):
        assert parse_date("15/03/26") == "2026-03-15"

    def test_portuguese_full(self):
        assert parse_date("15 de março de 2026") == "2026-03-15"

    def test_portuguese_short(self):
        result = parse_date("15 março", reference_year=2026)
        assert result == "2026-03-15"

    def test_day_abbrev(self):
        result = parse_date("SAB 15 MAR", reference_year=2026)
        assert result == "2026-03-15"

    def test_empty(self):
        assert parse_date("") is None

    def test_none(self):
        assert parse_date(None) is None

    def test_garbage(self):
        assert parse_date("não é uma data") is None


# ---------------------------------------------------------------------------
# PARSE TIME
# ---------------------------------------------------------------------------

class TestParseTime:
    def test_hh_mm(self):
        assert parse_time("20:30") == "20:30"

    def test_h_mm(self):
        assert parse_time("9:00") == "09:00"

    def test_h_format(self):
        assert parse_time("19h00") == "19:00"

    def test_h_only(self):
        assert parse_time("19h") == "19:00"

    def test_dot_format(self):
        assert parse_time("19.30") == "19:30"

    def test_pm(self):
        assert parse_time("7pm") == "19:00"

    def test_am(self):
        assert parse_time("9am") == "09:00"

    def test_12am(self):
        assert parse_time("12am") == "00:00"

    def test_empty(self):
        assert parse_time("") is None

    def test_none(self):
        assert parse_time(None) is None


# ---------------------------------------------------------------------------
# PARSE PRICE
# ---------------------------------------------------------------------------

class TestParsePrice:
    def test_free_entrada_livre(self):
        result = parse_price("Entrada livre")
        assert result["is_free"] is True

    def test_free_gratuito(self):
        result = parse_price("Gratuito")
        assert result["is_free"] is True

    def test_free_zero(self):
        result = parse_price("0,00€")
        assert result["is_free"] is True

    def test_single_price(self):
        result = parse_price("15€")
        assert result["is_free"] is False
        assert result["price_min"] == 15.0
        assert result["price_max"] == 15.0

    def test_range(self):
        result = parse_price("10€ a 25€")
        assert result["price_min"] == 10.0
        assert result["price_max"] == 25.0

    def test_decimal_comma(self):
        result = parse_price("12,50€")
        assert result["price_min"] == 12.5

    def test_discounts_detected(self):
        result = parse_price("15€ (desconto jovem 10€)")
        assert result["has_discounts"] is True

    def test_empty(self):
        result = parse_price("")
        assert result["is_free"] is False
        assert result["price_min"] is None

    def test_text_only(self):
        result = parse_price("Consultar bilheteira")
        assert result["price_display"] == "Consultar bilheteira"


# ---------------------------------------------------------------------------
# HARMONIZE CATEGORY
# ---------------------------------------------------------------------------

class TestHarmonizeCategory:
    def test_known_alias(self):
        result = harmonize_category(["música clássica"])
        assert result["domain"] == "musica"

    def test_unknown_falls_to_outros(self):
        result = harmonize_category(["xyzzy desconhecido"])
        assert result["domain"] == "outros"

    def test_empty_list(self):
        result = harmonize_category([])
        assert result["domain"] == "outros"


# ---------------------------------------------------------------------------
# HARMONIZE AUDIENCE
# ---------------------------------------------------------------------------

class TestHarmonizeAudience:
    def test_m_format(self):
        result = harmonize_audience("M/12")
        assert result["age_min"] == 12

    def test_empty(self):
        result = harmonize_audience("")
        assert result["age_min"] is None

    def test_raw_preserved(self):
        result = harmonize_audience("Maiores de 6 anos")
        assert result["label_raw"] == "Maiores de 6 anos"


# ---------------------------------------------------------------------------
# DETECT EVENT STATUS
# ---------------------------------------------------------------------------

class TestDetectEventStatus:
    def test_estreia(self):
        assert detect_event_status("Estreia Nacional", "", 3) == "estreia"

    def test_single_session(self):
        assert detect_event_status("Concerto", "", 1) == "unica-sessao"

    def test_normal(self):
        assert detect_event_status("Concerto", "", 5) == "em-cartaz"


# ---------------------------------------------------------------------------
# GENERATE EVENT ID
# ---------------------------------------------------------------------------

class TestGenerateEventId:
    def test_format(self):
        eid = generate_event_id("ccb", "Concerto de Natal", "2026-12-25")
        assert eid.startswith("ccb-2026-12-")
        assert "concerto" in eid

    def test_truncation(self):
        long_title = "A" * 200
        eid = generate_event_id("ccb", long_title, "2026-01-01")
        # O slug do título é truncado a 40 chars
        assert len(eid) < 60


# ---------------------------------------------------------------------------
# HARMONIZE EVENT (integração)
# ---------------------------------------------------------------------------

class TestHarmonizeEvent:
    def test_minimal_event(self):
        raw = {
            "title": "Concerto de Teste",
            "source_id": "test-123",
            "source_url": "https://example.com/event",
            "dates": [{"date": "2026-04-15", "time_start": "20:00"}],
            "categories": ["música"],
        }
        result = harmonize_event(raw, "ccb", "ccb")
        assert result["title"] == "Concerto de Teste"
        assert result["venue_id"] == "ccb"
        assert result["id"].startswith("ccb-")
        assert result["date_first"] == "2026-04-15"
        assert result["pipeline"]["is_active"] is True

    def test_preserves_source_id(self):
        raw = {
            "title": "Evento",
            "source_id": "abc-456",
            "source_url": "https://example.com",
            "dates": [{"date": "2026-05-01"}],
        }
        result = harmonize_event(raw, "tndm", "tndm")
        assert result["source_id"] == "abc-456"

    def test_accessibility_detection_lgp(self):
        raw = {
            "title": "Peça com LGP",
            "source_id": "x",
            "source_url": "https://example.com",
            "description": "Sessão com interpretação em LGP",
            "dates": [{"date": "2026-06-01"}],
        }
        result = harmonize_event(raw, "tndm", "tndm")
        assert result["accessibility"]["has_sign_language"] is True
