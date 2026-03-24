"""
Primeira Plateia — Taxonomia Canónica
Versão: 1.0 | 2026-03-24

Estrutura: DOMAINS → categories → subcategories + aliases para harmonização
"""

# ---------------------------------------------------------------------------
# DOMÍNIOS (Nível 1)
# ---------------------------------------------------------------------------
DOMAINS = {
    "musica": {
        "label": "Música",
        "slug": "musica",
        "emoji": "🎵",
        "color": "#457B9D",
        "order": 1,
    },
    "artes-palco": {
        "label": "Artes de Palco",
        "slug": "artes-palco",
        "emoji": "🎭",
        "color": "#E63946",
        "order": 2,
    },
    "artes-visuais": {
        "label": "Artes Visuais",
        "slug": "artes-visuais",
        "emoji": "🎨",
        "color": "#F4A261",
        "order": 3,
    },
    "pensamento": {
        "label": "Pensamento & Palavra",
        "slug": "pensamento",
        "emoji": "💬",
        "color": "#2A9D8F",
        "order": 4,
    },
    "cinema": {
        "label": "Cinema",
        "slug": "cinema",
        "emoji": "🎬",
        "color": "#264653",
        "order": 5,
    },
    "formacao": {
        "label": "Formação & Participação",
        "slug": "formacao",
        "emoji": "✏️",
        "color": "#8338EC",
        "order": 6,
    },
    "outros": {
        "label": "Outros",
        "slug": "outros",
        "emoji": "•",
        "color": "#6C757D",
        "order": 7,
    },
}

# ---------------------------------------------------------------------------
# CATEGORIAS (Nível 2) — por domínio
# ---------------------------------------------------------------------------
CATEGORIES = {

    # ── MÚSICA ──────────────────────────────────────────────────────────────
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

    # ── ARTES DE PALCO ──────────────────────────────────────────────────────
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

    # ── PENSAMENTO & PALAVRA ─────────────────────────────────────────────────
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

    # ── CINEMA ───────────────────────────────────────────────────────────────
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

    # ── FORMAÇÃO & PARTICIPAÇÃO ──────────────────────────────────────────────
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
        },
    },
}

# ---------------------------------------------------------------------------
# DICIONÁRIO DE ALIASES → categoria canónica
# Chave: texto raw (lowercase, sem acentos opcionais)
# Valor: (domain_slug, category_slug, subcategory_slug_ou_None, flags_dict)
# ---------------------------------------------------------------------------
ALIASES: dict[str, dict] = {

    # ── Música ───────────────────────────────────────────────────────────────
    "musica": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
    "música": {"domain": "musica", "category": "musica-classica", "subcategory": None, "flags": {}},
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

    # ── Artes de Palco ───────────────────────────────────────────────────────
    "teatro": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "theater": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "espetáculo de teatro": {"domain": "artes-palco", "category": "teatro", "subcategory": None, "flags": {}},
    "dança": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "danca": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "dance": {"domain": "artes-palco", "category": "danca", "subcategory": None, "flags": {}},
    "bailado": {"domain": "artes-palco", "category": "danca", "subcategory": "ballet", "flags": {}},
    "performance": {"domain": "artes-palco", "category": "performance", "subcategory": "performance-arte", "flags": {}},
    "humor": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},
    "stand-up": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},
    "comédia": {"domain": "artes-palco", "category": "circo-variedades", "subcategory": "humor", "flags": {}},

    # ── Artes Visuais ────────────────────────────────────────────────────────
    "exposição": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exposicoes": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exposições": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "exhibitions": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "mostra": {"domain": "artes-visuais", "category": "exposicoes", "subcategory": None, "flags": {}},
    "instalação": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "instalacao": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "installation": {"domain": "artes-visuais", "category": "instalacao", "subcategory": None, "flags": {}},
    "visita guiada": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visita-guiada": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-guiada", "flags": {}},
    "visita temática": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-tematica", "flags": {}},
    "visita-tematica": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-tematica", "flags": {}},
    "visita-jogo": {"domain": "artes-visuais", "category": "visitas", "subcategory": "visita-jogo", "flags": {}},
    "percurso": {"domain": "artes-visuais", "category": "visitas", "subcategory": "percurso", "flags": {}},

    # ── Pensamento & Palavra ─────────────────────────────────────────────────
    "conferência": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferencias": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferências": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": "conferencia", "flags": {}},
    "conferências e conversas": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},
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
    "clube de leitura": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "clube-leitura", "flags": {}},
    "apresentação de livro": {"domain": "pensamento", "category": "literatura-poesia", "subcategory": "apresentacao-livro", "flags": {}},
    "podcast": {"domain": "pensamento", "category": "podcast-radio", "subcategory": "podcast", "flags": {}},
    "pensamento": {"domain": "pensamento", "category": "conferencias-debates", "subcategory": None, "flags": {}},

    # ── Cinema ───────────────────────────────────────────────────────────────
    "cinema": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "cinemaen": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "filme": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "sessão de cinema": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "screening": {"domain": "cinema", "category": "cinema-ficcao", "subcategory": None, "flags": {}},
    "documentário": {"domain": "cinema", "category": "cinema-documental", "subcategory": "documentario", "flags": {}},

    # ── Formação ────────────────────────────────────────────────────────────
    "workshop": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "workshop-2": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "atelier": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "laboratório": {"domain": "formacao", "category": "workshops", "subcategory": None, "flags": {}},
    "oficina": {"domain": "formacao", "category": "oficinas", "subcategory": "oficina-criativa", "flags": {}},
    "curso": {"domain": "formacao", "category": "cursos", "subcategory": "curso-curto", "flags": {}},
    "formação": {"domain": "formacao", "category": "cursos", "subcategory": None, "flags": {}},
    "formacao": {"domain": "formacao", "category": "cursos", "subcategory": None, "flags": {}},
    "masterclass": {"domain": "formacao", "category": "cursos", "subcategory": "masterclass", "flags": {}},
    "academia": {"domain": "formacao", "category": "cursos", "subcategory": "curso-curto", "flags": {}},
    "residência artística": {"domain": "formacao", "category": "residencias", "subcategory": None, "flags": {}},
    "atividades de férias": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "atividades-ferias", "flags": {"is_family": True}},
    "atividades-de-ferias": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "atividades-ferias", "flags": {"is_family": True}},
    "escolas": {"domain": "formacao", "category": "atividades-educativas", "subcategory": "escolas", "flags": {"is_educational": True}},

    # ── Contextos que NÃO são categorias — ativam flags ─────────────────────
    "digital": {"domain": None, "category": None, "subcategory": None, "flags": {"is_digital": True}},
    "museu": {"domain": None, "category": None, "subcategory": None, "flags": {}},  # ignorar como categoria
    "cidade": {"domain": None, "category": None, "subcategory": None, "flags": {}},  # ignorar
    "atividades ao ar livre": {"domain": None, "category": None, "subcategory": None, "flags": {"is_outdoor": True}},
    "outdoor activities": {"domain": None, "category": None, "subcategory": None, "flags": {"is_outdoor": True}},
    "garagem sul": {"domain": None, "category": None, "subcategory": None, "flags": {}},        # espaço, não categoria
    "fábrica das artes": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Fábrica das Artes", "is_family": True}},
    "fabrica-das-artes": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Fábrica das Artes", "is_family": True}},
    "laboratório lipa": {"domain": None, "category": None, "subcategory": None, "flags": {"series_name": "Laboratório LIPA"}},
}

# ---------------------------------------------------------------------------
# CLASSIFICAÇÃO DE PÚBLICO → age_min, age_max, is_family, school_level
# ---------------------------------------------------------------------------
AUDIENCE_MAP: dict[str, dict] = {
    "m/0-3 anos": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "M/0-3 anos"},
    "bebés": {"age_min": 0, "age_max": 2, "is_family": True, "school_level": None, "label": "Bebés"},
    "primeira infância": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "Primeira Infância"},
    "primeira-infancia": {"age_min": 0, "age_max": 3, "is_family": True, "school_level": None, "label": "Primeira Infância"},
    "m/2-4": {"age_min": 2, "age_max": 4, "is_family": True, "school_level": None, "label": "M/2-4 anos"},
    "m/2-4 anos": {"age_min": 2, "age_max": 4, "is_family": True, "school_level": None, "label": "M/2-4 anos"},
    "m/3 anos": {"age_min": 3, "age_max": None, "is_family": True, "school_level": "pre-escolar", "label": "M/3 anos"},
    "m/3-5 anos": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/3-5 anos"},
    "m/4-5": {"age_min": 4, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/4-5 anos"},
    "m/4-5 anos": {"age_min": 4, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "M/4-5 anos"},
    "pré-escolar": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "Pré-Escolar"},
    "pre-escolar": {"age_min": 3, "age_max": 5, "is_family": True, "school_level": "pre-escolar", "label": "Pré-Escolar"},
    "m/6 anos": {"age_min": 6, "age_max": None, "is_family": True, "school_level": "primeiro-ciclo", "label": "M/6 anos"},
    "primeiro ciclo": {"age_min": 6, "age_max": 9, "is_family": True, "school_level": "primeiro-ciclo", "label": "Primeiro Ciclo"},
    "primeiro-ciclo": {"age_min": 6, "age_max": 9, "is_family": True, "school_level": "primeiro-ciclo", "label": "Primeiro Ciclo"},
    "m/6-12anos": {"age_min": 6, "age_max": 12, "is_family": True, "school_level": None, "label": "M/6-12 anos"},
    "m/6-12 anos": {"age_min": 6, "age_max": 12, "is_family": True, "school_level": None, "label": "M/6-12 anos"},
    "segundo ciclo": {"age_min": 10, "age_max": 12, "is_family": False, "school_level": "segundo-ciclo", "label": "Segundo Ciclo"},
    "segundo-ciclo": {"age_min": 10, "age_max": 12, "is_family": False, "school_level": "segundo-ciclo", "label": "Segundo Ciclo"},
    "terceiro ciclo e secundário": {"age_min": 13, "age_max": 17, "is_family": False, "school_level": "terceiro-ciclo", "label": "3.º Ciclo e Secundário"},
    "terceiro-ciclo-e-secundario": {"age_min": 13, "age_max": 17, "is_family": False, "school_level": "terceiro-ciclo", "label": "3.º Ciclo e Secundário"},
    "m/12 anos": {"age_min": 12, "age_max": None, "is_family": False, "school_level": None, "label": "M/12 anos"},
    "m/14 anos": {"age_min": 14, "age_max": None, "is_family": False, "school_level": None, "label": "M/14 anos"},
    "m/16 anos": {"age_min": 16, "age_max": None, "is_family": False, "school_level": None, "label": "M/16 anos"},
    "m/18 anos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "M/18 anos"},
    "universitário": {"age_min": 18, "age_max": None, "is_family": False, "school_level": "universitario", "label": "Universitário"},
    "universitario": {"age_min": 18, "age_max": None, "is_family": False, "school_level": "universitario", "label": "Universitário"},
    "para adultos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "Para Adultos"},
    "para-adultos": {"age_min": 18, "age_max": None, "is_family": False, "school_level": None, "label": "Para Adultos"},
    "para todos": {"age_min": 0, "age_max": None, "is_family": True, "school_level": None, "label": "Para Todos"},
    "para-todos": {"age_min": 0, "age_max": None, "is_family": True, "school_level": None, "label": "Para Todos"},
}

# ---------------------------------------------------------------------------
# SÉRIES PROGRAMÁTICAS CONHECIDAS (não são categorias)
# ---------------------------------------------------------------------------
KNOWN_SERIES = [
    "Sexta Maior", "Notas de Música", "Laboratório LIPA", "Fábrica das Artes",
    "Dias da Música", "Concerto Comentado", "Garagem Sul", "Música no Museu",
    "Ciclo de", "Série", "Temporada",
]

# ---------------------------------------------------------------------------
# PREFIXOS DE SÉRIE A EXTRAIR DO TÍTULO
# Ex: "Sexta Maior — Beethoven" → title="Beethoven", series_name="Sexta Maior"
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
]
