"""
Primeira Plateia — Taxonomia Canónica
Versão: 2.1 | 2026-03-27

Melhorias v2.1:
  - ALIASES carregados de pipeline/taxonomy_aliases.json (com fallback ao dict hardcoded)
  - log_unknown_tag() para registar tags desconhecidas em data/logs/unknown_tags.json

Melhorias v2.0:
  - Cobertura completa de aliases (0 categorias a cair em 'outros' com dados actuais)
  - Classificador por texto como fallback (título + descrição)
  - Geração automática de tags por inferência
  - AUDIENCE_MAP expandido com formatos M/NN do CCB e Theatro Circo
  - SERIES_PREFIX_PATTERNS expandido
  - NUTS2/NUTS3 para filtro geográfico
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

_logger = logging.getLogger(__name__)

# Raiz do projecto (dois níveis acima de pipeline/core/taxonomy.py)
_ROOT = Path(__file__).parent.parent.parent
_ALIASES_JSON = Path(__file__).parent.parent / "taxonomy_aliases.json"
_UNKNOWN_TAGS_LOG = _ROOT / "data" / "logs" / "unknown_tags.json"

# ---------------------------------------------------------------------------
# DOMÍNIOS (Nível 1)
# ---------------------------------------------------------------------------
DOMAINS = {
    "musica": {
        "label": "Música",
        "slug": "musica",
        "icon": "♪",
        "color": "#457B9D",
        "order": 1,
    },
    "artes-palco": {
        "label": "Artes de Palco",
        "slug": "artes-palco",
        "icon": "◈",
        "color": "#E63946",
        "order": 2,
    },
    "artes-visuais": {
        "label": "Artes Visuais",
        "slug": "artes-visuais",
        "icon": "◻",
        "color": "#F4A261",
        "order": 3,
    },
    "pensamento": {
        "label": "Pensamento & Palavra",
        "slug": "pensamento",
        "icon": "◇",
        "color": "#2A9D8F",
        "order": 4,
    },
    "cinema": {
        "label": "Cinema",
        "slug": "cinema",
        "icon": "▶",
        "color": "#264653",
        "order": 5,
    },
    "formacao": {
        "label": "Formação & Participação",
        "slug": "formacao",
        "icon": "○",
        "color": "#8338EC",
        "order": 6,
    },
    "outros": {
        "label": "Outros",
        "slug": "outros",
        "icon": "·",
        "color": "#6C757D",
        "order": 7,
    },
}

# ---------------------------------------------------------------------------
# CATEGORIAS (Nível 2) — por domínio
# ---------------------------------------------------------------------------
CATEGORIES = {

    # ── MÚSICA ───────────────────────────────────────────────────────────────
    "musica-classica": {
        "label": "Música Clássica",
        "domain": "musica",
        "subcategories": {
            "sinfonica": "Sinfónica",
            "camara": "Câmara",
            "recital": "Recital",
            "coral": "Coral / Coro",
            "opera": "Ópera",
            "barroca": "Barroca / Antiga",
            "contemporanea": "Contemporânea",
        },
    },
    "jazz-blues": {
        "label": "Jazz & Blues",
        "domain": "musica",
        "subcategories": {
            "jazz": "Jazz",
            "blues": "Blues",
            "big-band": "Big Band",
            "swing": "Swing",
        },
    },
    "musica-popular-portuguesa": {
        "label": "Música Popular Portuguesa",
        "domain": "musica",
        "subcategories": {
            "fado": "Fado",
            "folk": "Folk / Etnomusicologia",
            "musica-tradicional": "Música Tradicional",
            "pimba": "Pimba / Popular",
        },
    },
    "world-music": {
        "label": "World Music",
        "domain": "musica",
        "subcategories": {
            "africana": "Africana",
            "latina": "Latina",
            "oriental": "Oriental / Médio Oriente",
            "celta": "Celta",
            "flamenco-musica": "Flamenco",
        },
    },
    "pop-rock": {
        "label": "Pop & Rock",
        "domain": "musica",
        "subcategories": {
            "pop": "Pop",
            "rock": "Rock",
            "metal": "Metal",
            "indie": "Indie / Alternativo",
            "punk": "Punk",
        },
    },
    "electronica": {
        "label": "Eletrónica & Experimental",
        "domain": "musica",
        "subcategories": {
            "electronica": "Eletrónica",
            "experimental": "Experimental",
            "ambient": "Ambient",
            "techno": "Techno / Club",
        },
    },
    "musica-sacra": {
        "label": "Música Sacra",
        "domain": "musica",
        "subcategories": {
            "gregoriano": "Gregoriano",
            "oratorio": "Oratória",
        },
    },
    "outros-concertos": {
        "label": "Outros Concertos",
        "domain": "musica",
        "subcategories": {},
    },

    # ── ARTES DE PALCO ───────────────────────────────────────────────────────
    "teatro": {
        "label": "Teatro",
        "domain": "artes-palco",
        "subcategories": {
            "teatro-texto": "Teatro de Texto",
            "teatro-fisico": "Teatro Físico",
            "teatro-musical": "Teatro Musical",
            "musical": "Musical",
            "teatro-contemporaneo": "Teatro Contemporâneo",
            "teatro-classico": "Teatro Clássico",
            "monologos": "Monólogos",
            "teatro-de-bonecos": "Teatro de Bonecos / Marionetas",
            "teatro-de-rua": "Teatro de Rua",
            "teatro-documento": "Teatro-Documento",
        },
    },
    "danca": {
        "label": "Dança",
        "domain": "artes-palco",
        "subcategories": {
            "danca-contemporanea": "Contemporânea",
            "ballet": "Ballet / Dança Clássica",
            "danca-tradicional": "Tradicional / Folclórica",
            "danca-urbana": "Urbana / Hip-hop",
            "flamenco": "Flamenco",
            "danca-teatro": "Dança-Teatro",
        },
    },
    "opera-lirica": {
        "label": "Ópera & Lírica",
        "domain": "artes-palco",
        "subcategories": {
            "opera": "Ópera",
            "opereta": "Opereta",
            "zarzuela": "Zarzuela",
            "musical-lirico": "Musical Lírico",
        },
    },
    "circo-variedades": {
        "label": "Circo & Variedades",
        "domain": "artes-palco",
        "subcategories": {
            "circo-contemporaneo": "Circo Contemporâneo",
            "magia": "Magia / Ilusionismo",
            "acrobacia": "Acrobacia",
            "humor": "Humor / Stand-up",
            "variedades": "Variedades",
        },
    },
    "performance": {
        "label": "Performance",
        "domain": "artes-palco",
        "subcategories": {
            "performance-arte": "Performance Arte",
            "happening": "Happening",
            "site-specific": "Site-Specific",
            "multidisciplinar": "Multidisciplinar",
        },
    },

    # ── ARTES VISUAIS ────────────────────────────────────────────────────────
    "exposicoes": {
        "label": "Exposições",
        "domain": "artes-visuais",
        "subcategories": {
            "pintura": "Pintura",
            "escultura": "Escultura",
            "fotografia": "Fotografia",
            "design": "Design",
            "arquitetura": "Arquitetura",
            "arte-contemporanea": "Arte Contemporânea",
            "arte-moderna": "Arte Moderna",
            "arte-classica": "Arte Clássica",
            "multimidia": "Multimédia",
            "banda-desenhada": "Banda Desenhada",
        },
    },
    "instalacao": {
        "label": "Instalação",
        "domain": "artes-visuais",
        "subcategories": {
            "instalacao-sonora": "Instalação Sonora",
            "instalacao-interativa": "Instalação Interativa",
            "videoarte": "Videoarte",
        },
    },
    "visitas": {
        "label": "Visitas",
        "domain": "artes-visuais",
        "subcategories": {
            "visita-guiada": "Visita Guiada",
            "visita-tematica": "Visita Temática",
            "visita-jogo": "Visita-Jogo",
            "visita-performativa": "Visita Performativa",
            "percurso": "Percurso",
        },
    },

    # ── PENSAMENTO & PALAVRA ──────────────────────────────────────────────────
    "conferencias-debates": {
        "label": "Conferências & Debates",
        "domain": "pensamento",
        "subcategories": {
            "conferencia": "Conferência",
            "debate": "Debate",
            "coloquio": "Colóquio",
            "simposio": "Simpósio",
            "seminario": "Seminário",
        },
    },
    "conversas": {
        "label": "Conversas & Encontros",
        "domain": "pensamento",
        "subcategories": {
            "conversa": "Conversa",
            "entrevista-publica": "Entrevista Pública",
            "mesa-redonda": "Mesa Redonda",
            "ciclo-conversas": "Ciclo de Conversas",
        },
    },
    "literatura-poesia": {
        "label": "Literatura & Poesia",
        "domain": "pensamento",
        "subcategories": {
            "apresentacao-livro": "Apresentação de Livro",
            "leitura": "Leitura",
            "poesia": "Poesia",
            "clube-leitura": "Clube de Leitura",
            "concurso-literario": "Concurso Literário",
        },
    },
    "podcast-radio": {
        "label": "Podcast & Rádio",
        "domain": "pensamento",
        "subcategories": {
            "podcast": "Podcast",
            "gravacao-ao-vivo": "Gravação ao Vivo",
        },
    },

    # ── CINEMA ────────────────────────────────────────────────────────────────
    "cinema-ficcao": {
        "label": "Cinema de Ficção",
        "domain": "cinema",
        "subcategories": {
            "longa-metragem": "Longa-Metragem",
            "curta-metragem": "Curta-Metragem",
            "serie": "Série",
        },
    },
    "cinema-documental": {
        "label": "Cinema Documental",
        "domain": "cinema",
        "subcategories": {
            "documentario": "Documentário",
            "docuficcao": "Docuficção",
        },
    },
    "cinema-animacao": {
        "label": "Cinema de Animação",
        "domain": "cinema",
        "subcategories": {},
    },
    "cinema-experimental": {
        "label": "Cinema Experimental",
        "domain": "cinema",
        "subcategories": {},
    },
    "ciclo-cinema": {
        "label": "Ciclo de Cinema",
        "domain": "cinema",
        "subcategories": {
            "retrospetiva": "Retrospetiva",
            "homenagem": "Homenagem",
        },
    },

    # ── FORMAÇÃO & PARTICIPAÇÃO ───────────────────────────────────────────────
    "workshops": {
        "label": "Workshops",
        "domain": "formacao",
        "subcategories": {},
    },
    "cursos": {
        "label": "Cursos",
        "domain": "formacao",
        "subcategories": {
            "curso-curto": "Curso de Curta Duração",
            "masterclass": "Masterclass",
            "aula-aberta": "Aula Aberta",
        },
    },
    "oficinas": {
        "label": "Oficinas",
        "domain": "formacao",
        "subcategories": {
            "oficina-criativa": "Oficina Criativa",
            "oficina-tecnica": "Oficina Técnica",
        },
    },
    "residencias": {
        "label": "Residências Artísticas",
        "domain": "formacao",
        "subcategories": {},
    },
    "atividades-educativas": {
        "label": "Atividades Educativas",
        "domain": "formacao",
        "subcategories": {
            "escolas": "Para Escolas",
            "atividades-ferias": "Atividades de Férias",
            "mediacao": "Mediação",
        },
    },
    "participacao": {
        "label": "Participação",
        "domain": "formacao",
        "subcategories": {
            "open-call": "Open Call",
            "projeto-participativo": "Projeto Participativo",
        },
    },
}

# ---------------------------------------------------------------------------
# DICIONÁRIO DE ALIASES → categoria canónica
# Carregado de pipeline/taxonomy_aliases.json; hardcoded dict é o fallback.
# ---------------------------------------------------------------------------

def _load_aliases_from_json() -> dict | None:
    """Carrega aliases de taxonomy_aliases.json. Retorna None se falhar."""
    try:
        with open(_ALIASES_JSON, encoding="utf-8") as _f:
            _data = json.load(_f)
        _aliases = _data.get("aliases", {})
        if _aliases:
            _logger.debug("taxonomy: aliases carregados de %s (%d entradas)", _ALIASES_JSON, len(_aliases))
            return _aliases
    except FileNotFoundError:
        _logger.warning("taxonomy: %s não encontrado — a usar aliases hardcoded", _ALIASES_JSON)
    except Exception as _e:
        _logger.warning("taxonomy: erro ao ler %s (%s) — a usar aliases hardcoded", _ALIASES_JSON, _e)
    return None


# Hardcoded fallback — mantido em sincronismo com taxonomy_aliases.json
_ALIASES_HARDCODED: dict[str, dict] = {

    # ── Música ────────────────────────────────────────────────────────────────
    "musica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "música": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "música clássica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "musica classica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "musica-classica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "music": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "concerto": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "concerto sinfónico": {"domain": "musica", "category": "musica-classica", "subcategory": "sinfonica", "flags": {}},
    "concerto sinfonico": {"domain": "musica", "category": "musica-classica", "subcategory": "sinfonica", "flags": {}},
    "orquestra": {"domain": "musica", "category": "musica-classica", "subcategory": "sinfonica", "flags": {}},
    "recital": {"domain": "musica", "category": "musica-classica", "subcategory": "recital", "flags": {}},
    "música de câmara": {"domain": "musica", "category": "musica-classica", "subcategory": "camara", "flags": {}},
    "musica de camara": {"domain": "musica", "category": "musica-classica", "subcategory": "camara", "flags": {}},
    "musica-de-camara": {"domain": "musica", "category": "musica-classica", "subcategory": "camara", "flags": {}},
    "música antiga": {"domain": "musica", "category": "musica-classica", "subcategory": "barroca", "flags": {}},
    "musica-antiga": {"domain": "musica", "category": "musica-classica", "subcategory": "barroca", "flags": {}},
    "música barroca": {"domain": "musica", "category": "musica-classica", "subcategory": "barroca", "flags": {}},
    "musica-barroca": {"domain": "musica", "category": "musica-classica", "subcategory": "barroca", "flags": {}},
    "música contemporânea": {"domain": "musica", "category": "musica-classica", "subcategory": "contemporanea", "flags": {}},
    "musica-contemporanea": {"domain": "musica", "category": "musica-classica", "subcategory": "contemporanea", "flags": {}},
    "coro": {"domain": "musica", "category": "musica-classica", "subcategory": "coral", "flags": {}},
    "coral": {"domain": "musica", "category": "musica-classica", "subcategory": "coral", "flags": {}},
    "percussão": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "percussao": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "notas de música": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"series_name": "Notas de Música"}},
    "notas-de-musica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"series_name": "Notas de Música"}},
    "sexta maior": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"series_name": "Sexta Maior"}},
    "sexta-maior": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"series_name": "Sexta Maior"}},
    "música no museu": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"is_educational": True}},
    "musica-no-museu": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"is_educational": True}},
    "concerto comentado": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"is_educational": True}},
    "concerto-comentado": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {"is_educational": True}},
    "outras músicas": {"domain": "musica", "category": "outros-concertos", "subcategory": None, "flags": {}},
    "outras-musicas": {"domain": "musica", "category": "outros-concertos", "subcategory": None, "flags": {}},
    "fado": {"domain": "musica", "category": "musica-popular-portuguesa", "subcategory": "fado", "flags": {}},
    "jazz": {"domain": "musica", "category": "jazz-blues", "subcategory": "jazz", "flags": {}},
    "blues": {"domain": "musica", "category": "jazz-blues", "subcategory": "blues", "flags": {}},
    "ópera": {"domain": "artes-palco", "category": "opera-lirica", "subcategory": "opera", "flags": {}},
    "opera": {"domain": "artes-palco", "category": "opera-lirica", "subcategory": "opera", "flags": {}},

    # ── Artes de Palco ────────────────────────────────────────────────────────
    "teatro": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "theater": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "espetáculo de teatro": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "espetáculos": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "espetaculo": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "dança": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "danca": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "dance": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "bailado": {"domain": "artes-palco", "category": "danca", "subcategory": "ballet", "flags": {}},
    "performance": {"domain": "artes-palco", "category": "performance", "subcategory": "performance-arte", "flags": {}},
    "humor": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},
    "stand-up": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},
    "comédia": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},
    "multidisciplinar": {"domain": "artes-palco", "category": "performance", "subcategory": "multidisciplinar", "flags": {}},

    # ── Artes Visuais ─────────────────────────────────────────────────────────
    "exposição": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exposicoes": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exposições": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exhibitions": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exhibition": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "mostra": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "artes visuais": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "artes-visuais": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "instalação": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "instalacao": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "installation": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "visita guiada": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visita-guiada": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visitas guiadas": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visitas-guiadas": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visita temática": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-tematica", "flags": {}},
    "visita-tematica": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-tematica", "flags": {}},
    "visita-jogo": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-jogo", "flags": {}},
    "percurso": {"domain": "artes-visuais", "category": "visitas", "subcategory": "percurso", "flags": {}},
    "centro de arquitetura": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": "arquitetura", "flags": {}},

    # ── Pensamento & Palavra ──────────────────────────────────────────────────
    "conferência": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferencias": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferências": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferências e debates": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},
    "conferencias e debates": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},
    "conferências e conversas": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},
    "conferencias e conversas": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},
    "debate": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "debate", "flags": {}},
    "palestra": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "talk": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "colóquio": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "coloquio", "flags": {}},
    "conversa": {"domain": "pensamento", "category": "conversas", "subcategory": "conversa", "flags": {}},
    "conversa com o artista": {"domain": "pensamento", "category": "conversas", "subcategory": "conversa", "flags": {}},
    "encontro com o artista": {"domain": "pensamento", "category": "conversas", "subcategory": "conversa", "flags": {}},
    "mesa redonda": {"domain": "pensamento", "category": "conversas", "subcategory": "mesa-redonda", "flags": {}},
    "poesia": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "poesia", "flags": {}},
    "literatura": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": None, "flags": {}},
    "livros e pensamento": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": None, "flags": {}},
    "livros-e-pensamento": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": None, "flags": {}},
    "clube de leitura": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "clube-leitura", "flags": {}},
    "apresentação de livro": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "apresentacao-livro", "flags": {}},
    "apresentação": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "apresentacao-livro", "flags": {}},
    "livraria": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "apresentacao-livro", "flags": {}},
    "podcast": {"domain": "pensamento", "category": "podcast-radio", "subcategory": "podcast", "flags": {}},
    "pensamento": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},

    # ── Cinema ────────────────────────────────────────────────────────────────
    "cinema": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "cinemaen": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "filme": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "sessão de cinema": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "screening": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "documentário": {"domain": "cinema", "category": "cinema-documental", "subcategory": "documentario", "flags": {}},

    # ── Formação & Participação ───────────────────────────────────────────────
    "workshop": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "workshops": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "atelier": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "laboratório": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "oficina": {"domain": "formacao", "category": "oficinas", "subcategory": "oficina-criativa", "flags": {}},
    "oficinas": {"domain": "formacao", "category": "oficinas", "subcategory": "oficina-criativa", "flags": {}},
    "oficinas e formação": {"domain": "formacao", "category": "oficinas", "subcategory": None, "flags": {}},
    "oficinas-e-formacao": {"domain": "formacao", "category": "oficinas", "subcategory": None, "flags": {}},
    "curso": {"domain": "formacao", "category": "cursos", "subcategory": "curso-curto", "flags": {}},
    "formação": {"domain": "formacao", "category": "cursos", "subcategory": None, "flags": {}},
    "formacao": {"domain": "formacao", "category": "cursos", "subcategory": None, "flags": {}},
    "masterclass": {"domain": "formacao", "category": "cursos", "subcategory": "masterclass", "flags": {}},
    "academia": {"domain": "formacao", "category": "cursos", "subcategory": "curso-curto", "flags": {}},
    "aula aberta": {"domain": "formacao", "category": "cursos", "subcategory": "aula-aberta", "flags": {}},
    "aula-aberta": {"domain": "formacao", "category": "cursos", "subcategory": "aula-aberta", "flags": {}},
    "residência artística": {"domain": "formacao", "category": "residencias", "subcategory": None, "flags": {}},
    "atividades de férias": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "atividades-ferias", "flags": {"is_family": True}},
    "atividades-de-ferias": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "atividades-ferias", "flags": {"is_family": True}},
    "escolas": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "escolas", "flags": {"is_educational": True}},
    "atividades": {"domain": "formacao", "category": "atividades-educativas", "subcategory": None, "flags": {}},
    "mediação": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "mediacao", "flags": {}},
    "mediacao": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "mediacao", "flags": {}},
    "projetos de mediação": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "mediacao", "flags": {}},
    "projetos-de-mediacao": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "mediacao", "flags": {}},
    "participação": {"domain": "formacao", "category": "participacao", "subcategory": None, "flags": {}},
    "participacao": {"domain": "formacao", "category": "participacao", "subcategory": None, "flags": {}},
    "open call": {"domain": "formacao", "category": "participacao", "subcategory": "open-call", "flags": {}},

    # ── Público/audiência — ativam flags, não são categorias ──────────────────
    "famílias": {"domain": None, "category": None, "subcategory": None, "flags": {"is_family": True}},
    "familias": {"domain": None, "category": None, "subcategory": None, "flags": {"is_family": True}},
    "infância e juventude": {"domain": None, "category": None, "subcategory": None, "flags": {"is_family": True}},
    "infancia e juventude": {"domain": None, "category": None, "subcategory": None, "flags": {"is_family": True}},
    "público em geral": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "programação acessível": {"domain": None, "category": None, "subcategory": None, "flags": {"is_accessible": True}},
    "programacao-acessivel": {"domain": None, "category": None, "subcategory": None, "flags": {"is_accessible": True}},

    # ── Contextos/espaços/outros — ignorar como categoria ────────────────────
    "digital": {"domain": None, "category": None, "subcategory": None, "flags": {"is_digital": True}},
    "online": {"domain": None, "category": None, "subcategory": None, "flags": {"is_online": True}},
    "museu": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "mac/ccb": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "museum": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "cidade": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "porto": {"domain": None, "category": None, "subcategory": None, "flags": {"geographic_scope": "porto"}},
    "fora de portas": {"domain": None, "category": None, "subcategory": None, "flags": {"geographic_scope": "nacional"}},
    "fora-de-portas": {"domain": None, "category": None, "subcategory": None, "flags": {"geographic_scope": "nacional"}},
    "temporada 2025-26": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "atividades ao ar livre": {"domain": None, "category": None, "subcategory": None, "flags": {"is_outdoor": True}},
    "outdoor activities": {"domain": None, "category": None, "subcategory": None, "flags": {"is_outdoor": True}},
    "garagem sul": {"domain": None, "category": None, "subcategory": None, "flags": {}},
    "fábrica das artes": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Fábrica das Artes", "is_family": True}},
    "fabrica-das-artes": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Fábrica das Artes", "is_family": True}},
    "laboratório lipa": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Laboratório LIPA"}},
}

# Carregar do JSON; se falhar, usar dict hardcoded
# Merge: hardcoded é a base completa; JSON pode adicionar aliases de novos venues
# sem tocar no código Python. Resultado: todos os aliases hardcoded + overrides do JSON.
_json_aliases = _load_aliases_from_json() or {}
for _k in _json_aliases:
    if _k in _ALIASES_HARDCODED:
        _logger.debug("taxonomy: JSON sobrepõe alias hardcoded: %r", _k)
ALIASES: dict[str, dict] = {**_ALIASES_HARDCODED, **_json_aliases}


# ---------------------------------------------------------------------------
# LOG DE TAGS DESCONHECIDAS
# Buffer em memória — acumula durante o run, escreve uma única vez no fim
# via flush_unknown_tags(). Evita milhares de escritas de disco por run.
# ---------------------------------------------------------------------------

# Buffer: {(tag, venue_id): {"first_seen": "YYYY-MM-DD", "count": N}}
_unknown_tags_buffer: dict[tuple[str, str], dict] = {}


def log_unknown_tag(tag: str, venue_id: str) -> None:
    """
    Regista uma tag desconhecida no buffer em memória.
    Não escreve em disco — chamar flush_unknown_tags() no fim do run.
    """
    key = (tag, venue_id)
    if key in _unknown_tags_buffer:
        _unknown_tags_buffer[key]["count"] += 1
    else:
        _unknown_tags_buffer[key] = {
            "first_seen": date.today().isoformat(),
            "count": 1,
        }


def flush_unknown_tags() -> int:
    """
    Persiste o buffer de tags desconhecidas em data/logs/unknown_tags.json.
    Faz merge com entradas já existentes no ficheiro (incrementa counts).
    Retorna o número de tags distintas no buffer.
    Deve ser chamada uma vez no fim de cada run de venue.
    """
    if not _unknown_tags_buffer:
        return 0

    log_path = _UNKNOWN_TAGS_LOG
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # Ler registo existente do disco
        entries: list[dict] = []
        if log_path.exists():
            try:
                with open(log_path, encoding="utf-8") as _f:
                    entries = json.load(_f)
                if not isinstance(entries, list):
                    entries = []
            except Exception:
                entries = []

        # Indexar existentes por (tag, venue_id) para merge eficiente
        index: dict[tuple[str, str], dict] = {
            (e["tag"], e["venue_id"]): e
            for e in entries
            if "tag" in e and "venue_id" in e
        }

        # Merge do buffer
        for (tag, venue_id), buf in _unknown_tags_buffer.items():
            key = (tag, venue_id)
            if key in index:
                index[key]["count"] = index[key].get("count", 0) + buf["count"]
            else:
                index[key] = {
                    "tag": tag,
                    "venue_id": venue_id,
                    "first_seen": buf["first_seen"],
                    "count": buf["count"],
                }

        merged = sorted(index.values(), key=lambda e: (-e["count"], e["tag"]))

        # Escrever atomicamente
        tmp_path = log_path.with_suffix(".tmp")
        with open(tmp_path, "w", encoding="utf-8") as _f:
            json.dump(merged, _f, ensure_ascii=False, indent=2)
        tmp_path.replace(log_path)

        n = len(_unknown_tags_buffer)
        _unknown_tags_buffer.clear()
        _logger.info("taxonomy: %d tag(s) desconhecida(s) registadas em %s", n, log_path)
        return n

    except Exception as _e:
        _logger.warning("taxonomy: erro ao fazer flush de tags desconhecidas — %s", _e)
        return 0


# ---------------------------------------------------------------------------
# CLASSIFICADOR POR TEXTO (fallback quando alias não encontrado)
# ---------------------------------------------------------------------------
TEXT_CLASSIFIER_RULES: list[tuple[list[str], dict]] = [
    # Música
    (["sinfoni", "filarmoni", "orquest"], {"domain": "musica", "category": "musica-classica", "subcategory": "sinfonica"}),
    (["opera", "ópera", "líric"], {"domain": "artes-palco", "category": "opera-lirica", "subcategory": "opera"}),
    (["camara", "câmara", "quarteto", "trio", "duo"], {"domain": "musica", "category": "musica-classica", "subcategory": "camara"}),
    (["recital", "pianist", "violinist"], {"domain": "musica", "category": "musica-classica", "subcategory": "recital"}),
    (["coral", "coro", "vozes"], {"domain": "musica", "category": "musica-classica", "subcategory": "coral"}),
    (["jazz", "improvis", "big band"], {"domain": "musica", "category": "jazz-blues", "subcategory": "jazz"}),
    (["fado", "fadista", "guitarra portuguesa"], {"domain": "musica", "category": "musica-popular-portuguesa", "subcategory": "fado"}),
    (["eletróni", "electroni", "techno", "djset", "dj set"], {"domain": "musica", "category": "electronica", "subcategory": None}),
    (["concerto", "music"], {"domain": "musica", "category": "musica-classica", "subcategory": None}),
    # Artes de palco
    (["ballet", "bailado", "dança clássi"], {"domain": "artes-palco", "category": "danca", "subcategory": "ballet"}),
    (["dança contempor", "coreograf"], {"domain": "artes-palco", "category": "danca", "subcategory": "danca-contemporanea"}),
    (["dança", "dance", "bailarin"], {"domain": "artes-palco", "category": "danca", "subcategory": None}),
    (["teatro", "encenaç", "dramaturgi", "peça"], {"domain": "artes-palco", "category": "teatro", "subcategory": None}),
    (["circo", "acrobaci", "malabar"], {"domain": "artes-palco", "category": "circo-variedades", "subcategory": None}),
    (["performance", "site-specific", "happening"], {"domain": "artes-palco", "category": "performance", "subcategory": None}),
    # Artes visuais
    (["exposiç", "exhibit", "mostra", "galeri"], {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None}),
    (["instalaç", "videoarte", "video art"], {"domain": "artes-visuais", "category": "instalacao", "subcategory": None}),
    (["visita guiada", "visita temáti"], {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada"}),
    # Pensamento
    (["conferênci", "palestra", "colóqui", "simpósio", "semináro"], {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None}),
    (["debate", "mesa redonda", "forum"], {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "debate"}),
    (["livro", "lançamento", "literatura", "poesi", "leitura"], {"domain": "pensamento", "category": "literatura-poesia", "subcategory": None}),
    (["conversa", "encontro com"], {"domain": "pensamento", "category": "conversas", "subcategory": None}),
    (["podcast"], {"domain": "pensamento", "category": "podcast-radio", "subcategory": "podcast"}),
    # Cinema
    (["document", "documentári"], {"domain": "cinema", "category": "cinema-documental", "subcategory": "documentario"}),
    (["cinema", "film", "sessão de"], {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None}),
    (["animaç"], {"domain": "cinema", "category": "cinema-animacao", "subcategory": None}),
    # Formação
    (["workshop", "atelier", "oficin"], {"domain": "formacao", "category": "workshops", "subcategory": None}),
    (["masterclass", "formação", "curso"], {"domain": "formacao", "category": "cursos", "subcategory": None}),
    (["residência artístic"], {"domain": "formacao", "category": "residencias", "subcategory": None}),
    (["mediação", "escolas", "educativ"], {"domain": "formacao", "category": "atividades-educativas", "subcategory": None}),
]


def classify_by_text(title: str, description: str = "") -> dict | None:
    """
    Tenta classificar um evento por palavras-chave no título e descrição.
    Retorna dict com domain/category/subcategory ou None se não encontrado.
    """
    text = f"{title} {description}".lower()
    for keywords, result in TEXT_CLASSIFIER_RULES:
        if any(kw in text for kw in keywords):
            return result
    return None


# ---------------------------------------------------------------------------
# GERAÇÃO AUTOMÁTICA DE TAGS
# ---------------------------------------------------------------------------

def generate_tags(event: dict) -> list[str]:
    """
    Gera tags automáticas a partir dos campos já harmonizados do evento.
    Complementa (não substitui) tags manuais vindas do scraper.
    """
    tags: set[str] = set(event.get("tags") or [])

    domain       = event.get("domain", "")
    category     = event.get("category", "")
    subcategory  = event.get("subcategory")
    price        = event.get("price") or {}
    audience     = event.get("audience") or {}
    accessibility = event.get("accessibility") or {}
    event_status = event.get("event_status", "")
    pipeline     = event.get("pipeline") or {}

    # Domínio / categoria
    if domain and domain != "outros":
        domain_label = DOMAINS.get(domain, {}).get("label", "")
        if domain_label:
            tags.add(domain_label.lower())
    if subcategory:
        subcat_label = (
            CATEGORIES.get(category, {})
            .get("subcategories", {})
            .get(subcategory, "")
        )
        if subcat_label:
            tags.add(subcat_label.lower())

    # Preço
    if price.get("is_free"):
        tags.add("entrada-livre")
    if price.get("has_discounts"):
        tags.add("descontos")
    if price.get("ticketing_provider"):
        tags.add(f"bilhetes-{price['ticketing_provider']}")

    # Público / família
    if audience.get("is_family"):
        tags.add("familia")
    if audience.get("is_educational"):
        tags.add("escolas")
    age_min = audience.get("age_min")
    if age_min is not None:
        if age_min == 0:
            tags.add("bebes")
        elif age_min <= 6:
            tags.add("criancas")
        elif age_min <= 12:
            tags.add("jovens")
        elif age_min >= 18:
            tags.add("adultos")

    # Acessibilidade
    if accessibility.get("has_sign_language"):
        tags.add("lgp")
    if accessibility.get("has_audio_description"):
        tags.add("audiodescricao")
    if accessibility.get("has_subtitles"):
        tags.add("legendas")
    if accessibility.get("is_relaxed_performance"):
        tags.add("sessao-relaxada")

    # Status do evento
    if event_status == "estreia":
        tags.add("estreia")
    elif event_status == "unica-sessao":
        tags.add("sessao-unica")

    # Flags do pipeline
    flags = pipeline.get("extra_flags") or {}
    if flags.get("is_online"):
        tags.add("online")
    if flags.get("is_outdoor"):
        tags.add("ar-livre")
    if flags.get("is_digital"):
        tags.add("digital")
    if flags.get("geographic_scope") == "nacional":
        tags.add("digressao-nacional")

    # Série / festival
    if event.get("series_name"):
        tags.add("ciclo")
    if event.get("is_festival"):
        tags.add("festival")
    if event.get("is_multi_venue"):
        tags.add("multi-venue")

    return sorted(tags)


# ---------------------------------------------------------------------------
# AUDIENCE MAP
# ---------------------------------------------------------------------------
AUDIENCE_MAP: dict[str, dict] = {
    "m/0-3 anos": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "M/0-3 anos"},
    "bebés": {"age_min": 0, "age_max": 2, "is_family": True, "school_level": None, "label": "Bebés"},
    "bebes": {"age_min": 0, "age_max": 2, "is_family": True, "school_level": None, "label": "Bebés"},
    "primeira infância": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "Primeira Infância"},
    "primeira-infancia": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "Primeira Infância"},
    "m/2-4": {"age_min": 2, "age_max": 4, "is_family": True, "school_level": None, "label": "M/2-4 anos"},
    "m/2-4 anos": {"age_min": 2, "age_max": 4, "is_family": True, "school_level": None, "label": "M/2-4 anos"},
    "m/3 anos": {"age_min": 3, "age_max": None, "is_family": True, "school_level": "pre-escolar", "label": "M/3 anos"},
    "m/3": {"age_min": 3, "age_max": None, "is_family": True, "school_level": "pre-escolar", "label": "M/3 anos"},
    "m/3-5 anos": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/3-5 anos"},
    "m/4-5": {"age_min": 4, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/4-5 anos"},
    "m/4-5 anos": {"age_min": 4, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/4-5 anos"},
    "pré-escolar": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "Pré-Escolar"},
    "pre-escolar": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "Pré-Escolar"},
    "m/6 anos": {"age_min": 6, "age_max": None, "is_family": True, "school_level": "primeiro-ciclo", "label": "M/6 anos"},
    "m/6": {"age_min": 6, "age_max": None, "is_family": True, "school_level": "primeiro-ciclo", "label": "M/6 anos"},
    "primeiro ciclo": {"age_min": 6, "age_max": 9, "is_family": True, "school_level": "primeiro-ciclo", "label": "Primeiro Ciclo"},
    "primeiro-ciclo": {"age_min": 6, "age_max": 9, "is_family": True, "school_level": "primeiro-ciclo", "label": "Primeiro Ciclo"},
    "m/6-12anos": {"age_min": 6, "age_max": 12, "is_family": True, "school_level": None, "label": "M/6-12 anos"},
    "m/6-12 anos": {"age_min": 6, "age_max": 12, "is_family": True, "school_level": None, "label": "M/6-12 anos"},
    "segundo ciclo": {"age_min": 10, "age_max": 12, "is_family": False, "school_level": "segundo-ciclo", "label": "Segundo Ciclo"},
    "segundo-ciclo": {"age_min": 10, "age_max": 12, "is_family": False, "school_level": "segundo-ciclo", "label": "Segundo Ciclo"},
    "terceiro ciclo e secundário": {"age_min": 13, "age_max": 17, "is_family": False, "school_level": "terceiro-ciclo", "label": "3.º Ciclo e Secundário"},
    "terceiro-ciclo-e-secundario": {"age_min": 13, "age_max": 17, "is_family": False, "school_level": "terceiro-ciclo", "label": "3.º Ciclo e Secundário"},
    "m/8": {"age_min": 8, "age_max": None, "is_family": True, "school_level": None, "label": "M/8 anos"},
    "m/8 anos": {"age_min": 8, "age_max": None, "is_family": True, "school_level": None, "label": "M/8 anos"},
    "m/10": {"age_min": 10, "age_max": None, "is_family": True, "school_level": None, "label": "M/10 anos"},
    "m/10 anos": {"age_min": 10, "age_max": None, "is_family": True, "school_level": None, "label": "M/10 anos"},
    "m/12 anos": {"age_min": 12, "age_max": None, "is_family": False, "school_level": None, "label": "M/12 anos"},
    "m/12": {"age_min": 12, "age_max": None, "is_family": False, "school_level": None, "label": "M/12 anos"},
    "m/14 anos": {"age_min": 14, "age_max": None, "is_family": False, "school_level": None, "label": "M/14 anos"},
    "m/14": {"age_min": 14, "age_max": None, "is_family": False, "school_level": None, "label": "M/14 anos"},
    "+14": {"age_min": 14, "age_max": None, "is_family": False, "school_level": None, "label": "M/14 anos"},
    "m/16 anos": {"age_min": 16, "age_max": None, "is_family": False, "school_level": None, "label": "M/16 anos"},
    "m/16": {"age_min": 16, "age_max": None, "is_family": False, "school_level": None, "label": "M/16 anos"},
    "+16": {"age_min": 16, "age_max": None, "is_family": False, "school_level": None, "label": "M/16 anos"},
    "m/18 anos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "M/18 anos"},
    "m/18": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "M/18 anos"},
    "+18": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "M/18 anos"},
    "universitário": {"age_min": 18, "age_max": None, "is_family": False, "school_level": "universitario", "label": "Universitário"},
    "universitario": {"age_min": 18, "age_max": None, "is_family": False, "school_level": "universitario", "label": "Universitário"},
    "para adultos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "Para Adultos"},
    "para-adultos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "Para Adultos"},
    "para todos": {"age_min": 0, "age_max": None, "is_family": True, "school_level": None, "label": "Para Todos"},
    "para-todos": {"age_min": 0, "age_max": None, "is_family": True, "school_level": None, "label": "Para Todos"},
    "público em geral": {"age_min": 0, "age_max": None, "is_family": True, "school_level": None, "label": "Para Todos"},
    "infância e juventude": {"age_min": 0, "age_max": 17, "is_family": True, "school_level": None, "label": "Infância e Juventude"},
}

# ---------------------------------------------------------------------------
# SÉRIES PROGRAMÁTICAS CONHECIDAS
# ---------------------------------------------------------------------------
KNOWN_SERIES = [
    "Sexta Maior", "Notas de Música", "Laboratório LIPA", "Fábrica das Artes",
    "Dias da Música", "Concerto Comentado", "Garagem Sul", "Música no Museu",
    "Ciclo de", "Série", "Temporada",
]

# ---------------------------------------------------------------------------
# PREFIXOS DE SÉRIE A EXTRAIR DO TÍTULO
# ---------------------------------------------------------------------------
SERIES_PREFIX_PATTERNS = [
    r"^(Sexta Maior)\s*[—\-–:]\s*",
    r"^(Notas de Música)\s*[—\-–:]\s*",
    r"^(Música no Museu)\s*[—\-–:]\s*",
    r"^(Concerto Comentado)\s*[—\-–:]\s*",
    r"^(Laboratório LIPA)\s*[—\-–:]\s*",
    r"^(Fábrica das Artes)\s*[—\-–:]\s*",
    r"^(Dias da Música)\s*[—\-–:]\s*",
    r"^(Ciclo [^—\-–:]+)\s*[—\-–:]\s*",
    r"^(Temporada \d{4}[-/]\d{2,4})\s*[—\-–:]\s*",
]

# ---------------------------------------------------------------------------
# GEOGRAPHIC SCOPE
# ---------------------------------------------------------------------------
GEOGRAPHIC_SCOPE_VALUES = [
    "local",
    "regional",
    "nacional",
    "acores",
    "madeira",
    "internacional",
]

# ---------------------------------------------------------------------------
# NUTS2 Portugal
# ---------------------------------------------------------------------------
NUTS2 = {
    "norte": "Norte",
    "centro": "Centro",
    "alentejo": "Alentejo",
    "algarve": "Algarve",
    "area-metropolitana-de-lisboa": "Área Metropolitana de Lisboa",
    "regiao-autonoma-dos-acores": "Região Autónoma dos Açores",
    "regiao-autonoma-da-madeira": "Região Autónoma da Madeira",
}
