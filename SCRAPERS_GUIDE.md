# Palco Vivo — Guia de Criação de Scrapers

## Como adicionar um teatro novo

1. Cria o ficheiro `scrapers/scraper_<id>.py` com o bloco `THEATER` e a função `scrape()`
2. Faz push para `main`
3. O workflow **Sync Scrapers** dispara automaticamente e trata de tudo:
   - Actualiza `theaters.json`
   - Regista o scraper em `scraper.py`
   - O teatro aparece no frontend na próxima execução do pipeline diário (7h UTC)

Não precisas de editar mais nenhum ficheiro.

---

## Estrutura obrigatória de um scraper

Cada scraper tem **dois elementos obrigatórios**: o bloco `THEATER` e a função `scrape()`.

```python
"""
Scraper: Nome do Teatro
Fonte: https://www.nomedoteatro.pt/programacao
Cidade: Lisboa
"""

import re
import time
import requests
from bs4 import BeautifulSoup

from scrapers.utils import (
    make_id, log, HEADERS, can_scrape,
    truncate_synopsis, build_image_object,
)

# ─────────────────────────────────────────────────────────────
# THEATER — obrigatório, lido pelo sync automático
# ─────────────────────────────────────────────────────────────
THEATER = {
    "id":          "slug-unico",           # ex: "saoluiz", "viriato" — sem espaços
    "name":        "Nome Completo do Teatro",
    "short":       "Abreviatura",          # ex: "São Luiz", "CCB" — para chips e cards
    "color":       "#1565c0",              # cor hex para o frontend
    "city":        "Lisboa",
    "address":     "Rua X, 0000-000 Lisboa",
    "site":        "https://www.nomedoteatro.pt",
    "programacao": "https://www.nomedoteatro.pt/programacao",
    "lat":         38.7098,
    "lng":         -9.1421,
    "salas":       ["Grande Sala", "Sala Estúdio"],
    "aliases": [
        "nome do teatro",           # sempre incluir versão lowercase do name
        "abreviatura",              # sempre incluir versão lowercase do short
        "variante conhecida",       # outras formas como o teatro aparece nos dados
    ],
    "description": "Descrição do teatro para a página de teatros.",
}

# Convenção — usar sempre estes dois aliases
THEATER_NAME = THEATER["name"]
SOURCE_SLUG  = THEATER["id"]

BASE   = "https://www.nomedoteatro.pt"
AGENDA = f"{BASE}/programacao"


def scrape() -> list[dict]:
    """
    Ponto de entrada. Chamado pelo orquestrador (scraper.py).
    Deve devolver lista de dicts com os eventos do teatro.
    """
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []

    # lógica de scraping...
    events = []
    return events
```

---

## Campos obrigatórios no dict THEATER

| Campo | Tipo | Descrição |
|---|---|---|
| `id` | str | Identificador único. Só letras minúsculas, números e hífens. Ex: `"saoluiz"` |
| `name` | str | Nome canónico completo. Ex: `"São Luiz Teatro Municipal"` |
| `short` | str | Abreviatura para o frontend. Max ~15 chars. Ex: `"São Luiz"` |
| `color` | str | Cor hex para chips e cards. Ex: `"#1a73e8"` |
| `city` | str | Cidade. Ex: `"Lisboa"` |
| `site` | str | URL do site oficial |
| `programacao` | str | URL da página de programação |

## Campos recomendados no dict THEATER

| Campo | Tipo | Descrição |
|---|---|---|
| `address` | str | Morada completa |
| `lat` / `lng` | float | Coordenadas GPS |
| `salas` | list[str] | Nomes das salas |
| `aliases` | list[str] | Variações do nome em lowercase para normalização |
| `description` | str | Descrição para a página de teatros |

---

## Schema obrigatório do evento

Cada dict devolvido por `scrape()` deve ter estes campos:

| Campo | Tipo | Obrigatório | Descrição |
|---|---|---|---|
| `id` | str | ✅ | Usar `make_id(SOURCE_SLUG, title)` |
| `title` | str | ✅ | Título limpo do espetáculo |
| `theater` | str | ✅ | Usar sempre `THEATER_NAME` |
| `date_start` | str | ✅ | Formato ISO: `"2026-04-01"` |
| `source_url` | str | ✅ | URL canónico do evento no site do teatro |
| `synopsis` | str | recomendado | Usar `truncate_synopsis(texto)` |
| `image` | dict\|None | recomendado | Usar `build_image_object(url, soup, THEATER_NAME, source_url)` |
| `ticket_url` | str | recomendado | Link directo para compra de bilhetes |
| `date_end` | str | recomendado | Formato ISO: `"2026-04-15"` |
| `category` | str | recomendado | Do vocabulário controlado (ver abaixo) |
| `price_info` | str | opcional | Ex: `"10€ – 18€"` ou `"Entrada livre"` |
| `duration` | str | opcional | Ex: `"90 min."` |
| `age_rating` | str | opcional | Ex: `"+14"`, `"M/12"` |
| `schedule` | str | opcional | Horários textuais |
| `sala` | str | opcional | Nome da sala |
| `technical_sheet` | dict | opcional | Ficha técnica estruturada |

### Exemplo de evento bem formado

```python
return {
    "id":              make_id(SOURCE_SLUG, title),
    "title":           title,
    "theater":         THEATER_NAME,
    "category":        "Teatro",
    "dates_label":     "19 – 20 mar 2026",
    "date_start":      "2026-03-19",
    "date_end":        "2026-03-20",
    "schedule":        "Sex 21h00",
    "synopsis":        truncate_synopsis(synopsis_raw),
    "image":           build_image_object(img_url, soup, THEATER_NAME, url),
    "source_url":      url,
    "ticket_url":      "https://...",
    "price_info":      "10€ – 18€",
    "duration":        "90 min.",
    "age_rating":      "+14",
    "sala":            "Grande Sala",
    "technical_sheet": {"encenação": "Nome", "interpretação": "Elenco"},
}
```

---

## Vocabulário controlado de categorias

Usar sempre um destes valores no campo `category`.
O harmonizer normaliza automaticamente variações, mas é melhor usar o valor canónico directamente:

| Valor canónico | Variantes aceites pelo harmonizer |
|---|---|
| `"Teatro"` | `"teatro"`, `"theatre"`, `"peça"`, `"teatro contemporâneo"` |
| `"Dança"` | `"dança"`, `"dance"`, `"bailado"`, `"ballet"` |
| `"Ópera"` | `"ópera"`, `"opera"`, `"lírica"` |
| `"Teatro Musical"` | `"musical"`, `"teatro musical"` |
| `"Circo"` | `"circo"`, `"circo contemporâneo"`, `"acrobacia"` |
| `"Infanto-Juvenil"` | `"infantil"`, `"para famílias"`, `"para crianças"` |
| `"Performance"` | `"performance"`, `"performance art"` |
| `"Música"` | `"música"`, `"concerto"`, `"recital"` |
| `"Outro"` | fallback para qualquer valor não reconhecido |

---

## Funções utilitárias disponíveis em `scrapers/utils.py`

```python
from scrapers.utils import (
    make_id,            # make_id("slug", "Título do Espetáculo") → "slug-titulo-do-espetaculo"
    log,                # log("mensagem") — compatível com o sistema de logging
    HEADERS,            # dict com User-Agent ético para requests.get()
    can_scrape,         # can_scrape("https://...") → bool — verifica robots.txt
    truncate_synopsis,  # truncate_synopsis(texto, max_chars=300) → str truncado em frase completa
    build_image_object, # build_image_object(url, soup, theater_name, source_url) → dict|None
    parse_date,         # parse_date("15 março 2026") → "2026-03-15"
    parse_date_range,   # parse_date_range("15 a 20 março") → ("2026-03-15", "2026-03-20")
    MONTHS,             # dict meses PT/EN → int
)
```

### `build_image_object` — sempre usar para imagens

Nunca guardar imagens como string simples. Usar sempre:
```python
"image": build_image_object(img_url, soup, THEATER_NAME, source_url)
```
Devolve `{"url": ..., "credit": ..., "source": ..., "theater": ...}` ou `None`.

### `HEADERS` — sempre usar nos pedidos HTTP

```python
response = requests.get(url, headers=HEADERS, timeout=15)
```

### `can_scrape` — verificar robots.txt no início de `scrape()`

```python
def scrape():
    if not can_scrape(BASE):
        log(f"robots.txt: scraping bloqueado para {BASE}")
        return []
    # ...
```

---

## Boas práticas

### Delays entre pedidos
Respeitar os servidores dos teatros:
```python
time.sleep(0.3)   # entre páginas de eventos individuais
time.sleep(0.5)   # se o site for mais lento ou tiver muitas páginas
```

### Tratamento de erros por evento
Nunca deixar um erro num evento individual derrubar o scraper inteiro:
```python
for url in urls:
    try:
        ev = _scrape_event(url)
        if ev:
            events.append(ev)
    except Exception as e:
        log(f"[{THEATER_NAME}] Erro em {url}: {e}")
    time.sleep(0.3)
```

### Evitar duplicados internos
```python
seen_ids: set[str] = set()
for url in urls:
    ev = _scrape_event(url)
    if ev and ev["id"] not in seen_ids:
        seen_ids.add(ev["id"])
        events.append(ev)
```

### Datas — usar sempre ISO 8601
```python
"date_start": "2026-04-01"   # ✅ correcto
"date_start": "01/04/2026"   # ❌ o validator vai rejeitar
"date_start": "1 abril"      # ❌ o validator vai rejeitar
```
Usar `parse_date()` e `parse_date_range()` de `utils.py` para converter formatos textuais.

### Sinopse — truncar sempre no scraper
```python
"synopsis": truncate_synopsis(synopsis_raw)   # ✅
"synopsis": synopsis_raw                       # ❌ pode exceder limite
```

### Campos ausentes — preferir string vazia a None
```python
"price_info": price or ""     # ✅
"price_info": price or None   # ❌ pode causar avisos no validator
```

---

## Convenções de nomenclatura

| Elemento | Convenção | Exemplo |
|---|---|---|
| Nome do ficheiro | `scraper_<id>.py` | `scraper_viriato.py` |
| `SOURCE_SLUG` | igual ao `id` do THEATER | `"viriato"` |
| `make_id` prefix | igual ao `SOURCE_SLUG` | `make_id("viriato", title)` |
| Constante `BASE` | URL raiz do site | `"https://www.teatroviriato.com"` |
| Constante `AGENDA` | URL da listagem | `f"{BASE}/pt/programacao"` |
| Funções internas | prefixo `_` | `_scrape_event()`, `_parse_dates()` |

---

## Verificação antes de fazer push

Checklist rápido:

- [ ] O ficheiro chama-se `scraper_<id>.py`
- [ ] Tem dict `THEATER` com todos os campos obrigatórios
- [ ] Tem `THEATER_NAME = THEATER["name"]` e `SOURCE_SLUG = THEATER["id"]`
- [ ] Tem `def scrape() -> list[dict]:`
- [ ] Começa com `if not can_scrape(BASE): return []`
- [ ] Todos os `requests.get()` usam `headers=HEADERS, timeout=15`
- [ ] Imagens usam `build_image_object()`
- [ ] Sinopses usam `truncate_synopsis()`
- [ ] `date_start` em formato `"YYYY-MM-DD"`
- [ ] `source_url` preenchido em todos os eventos
- [ ] `theater` usa sempre `THEATER_NAME`
- [ ] `id` usa sempre `make_id(SOURCE_SLUG, title)`

---

## Como ajustar dados de um teatro depois do registo

Se quiseres alterar a cor, descrição ou outros metadados de um teatro já registado:

**Opção A — Editar `theaters.json` directamente** (recomendado para ajustes visuais)
1. Abre `theaters.json` no GitHub (edição inline no browser)
2. Altera o campo desejado
3. Adiciona o nome do campo ao array `_overrides` da entrada do teatro
4. Commit — o campo fica protegido de sobrescrita pelo sync automático

```json
{
  "id": "viriato",
  "color": "#0d47a1",
  "_overrides": ["color"]
}
```

**Opção B — Editar o dict `THEATER` no scraper**
1. Altera o valor no `scraper_<id>.py`
2. Faz push — o sync automático actualiza `theaters.json`
3. Funciona para campos que não estejam em `_overrides`

---

## Como remover um teatro

1. Apaga o ficheiro `scrapers/scraper_<id>.py`
2. Remove a entrada de `theaters.json` manualmente
3. Remove o import e a linha de `SCRAPERS` em `scraper.py` manualmente
   (o sync automático só adiciona, nunca remove — por segurança)
