"""
OLX scraper -> tabela `listings` (SQLite).

Korzysta WYŁĄCZNIE z publicznego endpointu JSON  https://www.olx.pl/api/v1/offers/
(ten sam, który woła strona OLX; w robots.txt jest jawnie dozwolony: "Allow: /api/v1/offers/").
Nie pobiera HTML-owych stron ogłoszeń, więc requestów jest mało (40 ogłoszeń / request,
a opis jest już w odpowiedzi).

Zasady "bezpiecznego" scrapowania wbudowane w skrypt:
  * losowy odstęp między requestami (domyślnie 4-8 s), jedna sesja, jeden wątek
  * twardy limit stron na zapytanie i requestów na uruchomienie
  * Retry-After / exponential backoff na 429 i 5xx
  * 403 / powtarzające się 429  => skrypt SIĘ ZATRZYMUJE (nie próbuje obchodzić blokady)
  * tryb --incremental: sortowanie od najnowszych, stop gdy strona nie ma nic nowego
  * UPSERT po URL (nie kasuje wierszy, więc nie rwie relacji do cpu/gpu/ram...)

Kontakt do User-Agenta (opcjonalnie) podajesz przez zmienną środowiskową
OLX_SCRAPER_CONTACT albo plik .env obok skryptu (dodaj .env do .gitignore!).

Użycie:
  python olx_scraper.py --dry-run                 # tylko 1 strona 1. zapytania, bez zapisu
  python olx_scraper.py                           # pełny przebieg wg QUERIES
  python olx_scraper.py --incremental             # codzienne odświeżanie (mało requestów)
  python olx_scraper.py -q "ryzen 5 3600" --max-pages 2
"""
import argparse
import html
import logging
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests


# ----------------------------------------------------------------- KONFIG
def _load_dotenv(path=Path(__file__).resolve().parent / ".env"):
    """Minimalny loader .env (bez zależności). Plik .env ma być w .gitignore!"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

BASE_URL = "https://www.olx.pl/api/v1/offers/"
# Kontakt NIE siedzi w kodzie - bierzemy ze zmiennej środowiskowej / pliku .env
CONTACT = os.environ.get("OLX_SCRAPER_CONTACT", "").strip()
USER_AGENT = "homelab-listings-research/0.1 (personal, non-commercial" + (
    f"; {CONTACT})" if CONTACT else ")"
)

# 99 = Elektronika (główna kategoria). Węższą (Komputery/Laptopy) podejrzysz w DevTools ->
# Network -> request do /api/v1/offers/ na stronie olx.pl/elektronika/komputery/ -> category_id
CATEGORY_ID = 99

QUERIES = [
    "procesor intel", "procesor ryzen",
    "płyta główna", "pamięć ram ddr4", "pamięć ram ddr5", "ram sodimm",
    "karta graficzna", "dysk ssd", "dysk hdd", "zasilacz atx",
    "mini pc", "dell optiplex", "lenovo thinkcentre", "hp elitedesk", "laptop",
]

PAGE_LIMIT = 40            # ile ogłoszeń na request
MAX_PAGES_PER_QUERY = 5    # twardy bezpiecznik (5 * 40 = 200 ogłoszeń / zapytanie)
MAX_REQUESTS_PER_RUN = 150 # twardy bezpiecznik na cały przebieg
DELAY_RANGE = (4.0, 8.0)   # sekundy między requestami
QUERY_PAUSE = (15.0, 30.0) # dłuższa przerwa między zapytaniami
TIMEOUT = 20
MAX_RETRIES = 3

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "listings.db"

log = logging.getLogger("olx")


class Blocked(Exception):
    """OLX nas blokuje / ogranicza - kończymy przebieg."""


# ----------------------------------------------------------------- HTTP
class Fetcher:
    def __init__(self, delay=DELAY_RANGE, max_requests=MAX_REQUESTS_PER_RUN):
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "pl-PL,pl;q=0.9",
        })
        self.delay = delay
        self.max_requests = max_requests
        self.count = 0
        self._last = 0.0
        self._strikes = 0  # kolejne 429

    def _sleep_until_allowed(self):
        wait = random.uniform(*self.delay) - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)

    def get_json(self, params):
        if self.count >= self.max_requests:
            raise Blocked(f"Osiągnięto limit {self.max_requests} requestów na przebieg")

        for attempt in range(1, MAX_RETRIES + 1):
            self._sleep_until_allowed()
            self.count += 1
            try:
                r = self.s.get(BASE_URL, params=params, timeout=TIMEOUT)
            except requests.RequestException as e:
                self._last = time.monotonic()
                log.warning("Błąd sieci (%s), próba %d/%d", e, attempt, MAX_RETRIES)
                time.sleep(10 * 2 ** (attempt - 1))
                continue
            self._last = time.monotonic()

            if r.status_code == 200:
                self._strikes = 0
                try:
                    return r.json()
                except ValueError:
                    raise Blocked("Odpowiedź 200, ale nie JSON (captcha / zmiana API?)")

            if r.status_code == 403:
                raise Blocked("HTTP 403 - dostęp zablokowany. Nie obchodzę tego; zatrzymuję się.")

            if r.status_code == 429:
                self._strikes += 1
                if self._strikes >= 2:
                    raise Blocked("Powtarzające się HTTP 429 - zatrzymuję się.")
                retry_after = r.headers.get("Retry-After", "")
                wait = int(retry_after) if retry_after.isdigit() else 120
                wait = min(wait, 600)
                log.warning("HTTP 429, czekam %ds", wait)
                time.sleep(wait)
                continue

            if 500 <= r.status_code < 600:
                wait = 15 * 2 ** (attempt - 1)
                log.warning("HTTP %d, czekam %ds (próba %d/%d)", r.status_code, wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue

            log.error("Nieoczekiwany HTTP %d: %s", r.status_code, r.text[:200])
            return None

        log.error("Wyczerpano próby dla params=%s", params)
        return None


# ----------------------------------------------------------------- PARSOWANIE
_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"[ \t\r\f\v]+")


def clean_description(raw):
    if not raw:
        return None
    text = re.sub(r"<br\s*/?>", "\n", raw, flags=re.I)
    text = _TAG_RE.sub("", text)
    text = html.unescape(text)
    text = _WS_RE.sub(" ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text).strip()
    return text or None


def extract_price(offer):
    """Zwraca float albo None (np. 'zamienię', 'oddam za darmo' => None / 0)."""
    for p in offer.get("params") or []:
        if p.get("key") == "price":
            v = p.get("value") or {}
            if v.get("type") == "free":
                return 0.0
            if v.get("type") == "exchange":
                return None
            val = v.get("value")
            if val is None:
                return None
            if v.get("currency") not in (None, "PLN"):
                log.debug("Waluta %s dla %s - pomijam cenę", v.get("currency"), offer.get("url"))
                return None
            try:
                return round(float(val), 2)
            except (TypeError, ValueError):
                return None
    return None


def extract_location(offer):
    loc = offer.get("location") or {}
    parts = []
    for key in ("city", "district", "region"):
        name = (loc.get(key) or {}).get("name")
        if name and name not in parts:
            parts.append(name)
    return ", ".join(parts)[:100] or None


def parse_offer(offer):
    url = offer.get("url")
    if not url:
        return None
    url = url.split("#")[0]  # bez fragmentów typu #xxxx;promoted
    return {
        "platform": "OLX",
        "url": url,
        "title": (offer.get("title") or "").strip(),
        "price": extract_price(offer),
        "raw_description": clean_description(offer.get("description")),
        "seller_location": extract_location(offer),
    }


# ----------------------------------------------------------------- DB
def connect(db_path):
    db_path = Path(db_path)
    if not db_path.exists():
        sys.exit(f"Brak bazy {db_path} - najpierw uruchom db_setup.py (albo podaj --db).")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")  # w SQLite domyślnie OFF -> ON DELETE CASCADE nie działałoby
    return conn


UPSERT_SQL = """
INSERT INTO listings (platform, url, title, price, raw_description, seller_location, is_active, scraped_at)
VALUES (:platform, :url, :title, :price, :raw_description, :seller_location, 1, CURRENT_TIMESTAMP)
ON CONFLICT(url) DO UPDATE SET
    title           = excluded.title,
    price           = excluded.price,
    raw_description = COALESCE(excluded.raw_description, listings.raw_description),
    seller_location = COALESCE(excluded.seller_location, listings.seller_location),
    is_active       = 1,
    scraped_at      = CURRENT_TIMESTAMP
"""


def known_urls(conn, urls):
    if not urls:
        return set()
    q = ",".join("?" * len(urls))
    return {r[0] for r in conn.execute(f"SELECT url FROM listings WHERE url IN ({q})", urls)}


# ----------------------------------------------------------------- GŁÓWNA PĘTLA
def scrape_query(fetcher, conn, query, max_pages, incremental, dry_run):
    new_total = upd_total = 0
    offset = 0
    for page in range(1, max_pages + 1):
        params = {
            "offset": offset,
            "limit": PAGE_LIMIT,
            "query": query,
            "category_id": CATEGORY_ID,
            "sort_by": "created_at:desc",
        }
        js = fetcher.get_json(params)
        if not js:
            break
        data = js.get("data") or []
        if not data:
            break

        parsed = [p for p in (parse_offer(o) for o in data) if p]
        urls = [p["url"] for p in parsed]
        already = known_urls(conn, urls)
        new_count = sum(1 for u in urls if u not in already)

        if dry_run:
            for p in parsed[:5]:
                log.info("DRY  %s | %s zł | %s | %s", p["title"][:60], p["price"], p["seller_location"], p["url"])
        else:
            conn.executemany(UPSERT_SQL, parsed)
            conn.commit()

        new_total += new_count
        upd_total += len(parsed) - new_count
        log.info("[%s] strona %d: %d ogł. (%d nowych)", query, page, len(parsed), new_count)

        if dry_run:
            break
        if incremental and new_count == 0:
            log.info("[%s] --incremental: nic nowego, kończę to zapytanie", query)
            break

        links = js.get("links")
        if len(data) < PAGE_LIMIT or (isinstance(links, dict) and not links.get("next")):
            break
        offset += PAGE_LIMIT
    return new_total, upd_total


def main():
    ap = argparse.ArgumentParser(description="OLX -> SQLite scraper")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("-q", "--query", action="append", help="własne zapytanie (można wielokrotnie); domyślnie QUERIES")
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_QUERY)
    ap.add_argument("--incremental", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="1 request, bez zapisu do bazy")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if not CONTACT:
        log.warning("Brak OLX_SCRAPER_CONTACT (.env / zmienna środowiskowa) - User-Agent bez kontaktu.")

    queries = args.query or QUERIES
    if args.dry_run:
        queries = queries[:1]

    conn = connect(args.db)
    fetcher = Fetcher()
    total_new = total_upd = 0
    try:
        for i, q in enumerate(queries):
            n, u = scrape_query(fetcher, conn, q, args.max_pages, args.incremental, args.dry_run)
            total_new += n
            total_upd += u
            if i < len(queries) - 1:
                time.sleep(random.uniform(*QUERY_PAUSE))
    except Blocked as e:
        log.error("PRZERWANO: %s", e)
    except KeyboardInterrupt:
        log.warning("Przerwane ręcznie")
    finally:
        conn.close()
        log.info("Koniec: %d requestów, %d nowych, %d zaktualizowanych", fetcher.count, total_new, total_upd)


if __name__ == "__main__":
    main()
