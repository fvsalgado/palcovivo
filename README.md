# Primeira Plateia
**A Cultura no Lugar Certo** · [primeiraplateia.pt](https://primeiraplateia.pt)

Portal de agenda cultural portuguesa. Sem publicidade. Sem custos. Um projeto de Fábio Salgado.

---

## Arquitectura

```
primeira-plateia/
├── .github/workflows/
│   └── daily-pipeline.yml      # GitHub Actions — corre às 06:00 UTC
├── pipeline/
│   ├── scrapers/
│   │   └── {venue-id}/
│   │       └── scraper.py      # Um scraper por venue
│   ├── core/
│   │   ├── taxonomy.py         # Taxonomia canónica + aliases
│   │   ├── harmonizer.py       # Harmonizador central
│   │   ├── validator.py        # Validação contra schema
│   │   ├── dedup.py            # Deduplicação inter-venues
│   │   └── cache.py            # Cache local (TTL 23h)
│   ├── utils/
│   │   └── notify.py           # Email HTML + Ntfy
│   ├── run_venue.py             # Scrape de venue único
│   └── aggregate.py             # Agregação + master.json
├── data/
│   ├── venues/                 # {venue-id}.json — 1 ficheiro por venue
│   ├── events/                 # {venue-id}.json — eventos por venue
│   ├── cache/                  # Cache raw dos scrapers
│   ├── logs/                   # Relatórios de execução
│   └── master.json             # Output final para o site
├── schemas/
│   ├── venue.schema.json       # JSON Schema de venue (referência/documentação)
│   └── event.schema.json       # JSON Schema de evento (referência/documentação)
├── site/                       # Frontend Astro (a construir)
├── admin/
│   └── index.html              # Interface de gestão local
└── requirements.txt
```

---

## Pipeline

```
[Fontes: APIs / RSS / HTML]
        ↓
[Scrapers individuais por venue]
        ↓
[Cache local — TTL 23h]
        ↓
[Harmonizador — datas, preços, categorias, títulos]
        ↓
[Validador — schema + regras de negócio]
        ↓
[Deduplicador — merge de sessões + cross-venue]
        ↓
[data/master.json — output para o site]
        ↓
[GitHub Actions commit → GitHub Pages build]
        ↓
[Notificações: Email HTML + Ntfy]
```

---

## Venues activos

| ID | Nome | Tipo API | Estado |
|----|------|----------|--------|
| `ccb` | Centro Cultural de Belém | REST (Events Calendar) | ✅ |

---

## Adicionar um novo venue

1. Criar `data/venues/{venue-id}.json` com dados do venue
2. Criar `pipeline/scrapers/{venue-id}/scraper.py` com função `run() → list[dict]`
3. Testar: `PYTHONPATH=. python pipeline/run_venue.py <venue-id> --force`

Consulta `pipeline/scrapers/ccb/scraper.py` como template.

---

## GitHub Secrets necessários

| Secret | Descrição |
|--------|-----------|
| `GMAIL_USER` | Endereço Gmail |
| `GMAIL_APP_PASSWORD` | App Password Gmail |
| `NOTIFY_EMAIL` | Email destino |
| `NTFY_URL` | URL canal Ntfy |

---

## Correr localmente

```bash
# Instalar dependências
pip install -r requirements.txt

# Scrape de um venue
PYTHONPATH=. python pipeline/run_venue.py ccb

# Forçar refresh sem cache
PYTHONPATH=. python pipeline/run_venue.py ccb --force

# Agregar todos os venues + gerar master.json
PYTHONPATH=. python pipeline/aggregate.py

# Enviar notificações com o último relatório
python -m pipeline.utils.notify
```

---

## Taxonomia

8 domínios · ~25 categorias · ~80 subcategorias · +200 aliases mapeados.

Ver `pipeline/core/taxonomy.py` e `docs/taxonomy.md`.

---

*Projeto de Fábio Salgado · fabio@primeiraplateia.pt*
