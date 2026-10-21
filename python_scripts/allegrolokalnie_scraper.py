"""
Allegro Lokalnie -> tabela `listings` (SQLite).

Jak to działa:
  * pobiera zwykłą stronę wyszukiwania  https://allegrolokalnie.pl/oferty/q/<fraza>?sort=startingTime-desc
    (HTML renderowany po stronie serwera - bez JavaScriptu)
  * dane bierze z dwóch miejsc na stronie: JSON-LD (schema.org: nazwa, URL, cena, STAN przedmiotu)
    oraz kafelków <article> (lokalizacja, parametry, typ oferty, sprzedawca)
  * 1 request = do 60 ogłoszeń; NIE otwiera stron pojedynczych ofert
  * pomija przedmioty NOWE (interesują nas używki) - decyduje stan z JSON-LD
  * przed każdym requestem sprawdza robots.txt serwisu (fail-closed: bez robots.txt nie scrapuje)

Typy ofert na Lokalnie (zapisywane w raw_description):
  kup teraz | ogłoszenie (często "do negocjacji") | licytacja (cena = BIEŻĄCA oferta, nie końcowa)
  oferta z Allegro (to oferta z allegro.pl - URL normalizowany do https://allegro.pl/oferta/<id>,
                    platform='Allegro', żeby nie dublować się ze scraperem Allegro)

Bezpieczeństwo (twarde zasady):
  * 8-15 s między requestami, 30-60 s między zapytaniami, jeden wątek
  * limity: 3 strony / zapytanie, 60 requestów / przebieg
  * 403 / 429 / captcha / strona bez struktury listingu / przekierowanie  => STOP, bez obchodzenia

Tryby:
  python allegrolokalnie_scraper.py --from-html "plik.html"   # parsuje zapisaną stronę, 0 requestów
  python allegrolokalnie_scraper.py --dry-run                 # 1 request online, bez zapisu
  python allegrolokalnie_scraper.py                           # pełny przebieg wg QUERIES
  python allegrolokalnie_scraper.py --incremental             # codzienne odświeżanie

Kontakt do User-Agenta: ALLEGRO_SCRAPER_CONTACT / OLX_SCRAPER_CONTACT (zmienna środowiskowa lub .env).
"""
import argparse
import html as htmllib
import json
import logging
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import quote, urlencode, urlparse

import requests


# ----------------------------------------------------------------- KONFIG
def _load_dotenv(path=Path(__file__).resolve().parent / ".env"):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_dotenv()

HOST = "https://allegrolokalnie.pl"
CONTACT = (os.environ.get("ALLEGRO_SCRAPER_CONTACT") or os.environ.get("OLX_SCRAPER_CONTACT") or "").strip()
USER_AGENT = "homelab-listings-research/0.1 (personal, non-commercial" + (f"; {CONTACT})" if CONTACT else ")")

SORT = "startingTime-desc"   # najnowsze (wartość z listy sortowania na stronie)
SOURCES = ["lokalnie"]       # ["lokalnie", "allegro"] - "allegro" dubluje to, co łapie allegro_scraper.py
SKIP_NEW = True              # pomijaj przedmioty NOWE (schema.org NewCondition)

QUERIES = [
    "procesor ryzen", "procesor intel core", "płyta główna am4", "płyta główna lga1151",
    "pamięć ram ddr4", "ram sodimm ddr4", "karta graficzna", "dysk ssd", "dysk hdd 3.5",
    "zasilacz atx", "mini pc", "dell optiplex", "lenovo thinkcentre", "hp elitedesk", "laptop",
]

MAX_PAGES_PER_QUERY = 3
MAX_REQUESTS_PER_RUN = 60
DELAY_RANGE = (8.0, 15.0)
QUERY_PAUSE = (30.0, 60.0)
TIMEOUT = 25
MAX_RETRIES = 2

DEFAULT_DB = Path(__file__).resolve().parent.parent / "data" / "listings.db"

log = logging.getLogger("lokalnie")


class Blocked(Exception):
    """Serwis nas ogranicza / zmienił format - kończymy przebieg."""


# ----------------------------------------------------------------- ROBOTS.TXT
class Robots:
    """Minimalny parser robots.txt (grupa `User-agent: *`, wildcardy `*` i `$`, wygrywa najdłuższa reguła)."""

    def __init__(self, text=""):
        self.rules = []
        in_star = seen_rule = False
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            key, val = (x.strip() for x in line.split(":", 1))
            key = key.lower()
            if key == "user-agent":
                if seen_rule:
                    in_star = seen_rule = False
                if val == "*":
                    in_star = True
            elif key in ("allow", "disallow") and in_star:
                seen_rule = True
                if val:
                    pat = re.escape(val).replace(r"\*", ".*")
                    if pat.endswith(r"\$"):
                        pat = pat[:-2] + "$"
                    self.rules.append((key == "allow", re.compile(pat), len(val)))

    def allowed(self, path_and_query):
        best = None
        for is_allow, rx, ln in self.rules:
            if rx.match(path_and_query) and (best is None or ln > best[1] or (ln == best[1] and is_allow)):
                best = (is_allow, ln)
        return True if best is None else best[0]


# ----------------------------------------------------------------- HTTP
class Fetcher:
    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "pl-PL,pl;q=0.9",
        })
        self.count = 0
        self._last = 0.0
        self._strikes = 0
        self.robots = Robots()

    def _wait(self):
        wait = random.uniform(*DELAY_RANGE) - (time.monotonic() - self._last)
        if wait > 0:
            time.sleep(wait)

    def load_robots(self):
        try:
            r = self.s.get(HOST + "/robots.txt", timeout=TIMEOUT)
            self._last = time.monotonic()
            if r.status_code == 200 and "user-agent" in r.text.lower():
                self.robots = Robots(r.text)
                log.info("robots.txt wczytany (%d reguł)", len(self.robots.rules))
                return
            if r.status_code in (403, 429):
                raise Blocked(f"robots.txt -> HTTP {r.status_code}")
        except requests.RequestException as e:
            log.warning("Nie udało się pobrać robots.txt: %s", e)
        raise Blocked("Brak robots.txt - nie scrapuję 'na ślepo'.")

    def get_html(self, url):
        p = urlparse(url)
        if not self.robots.allowed(p.path + ("?" + p.query if p.query else "")):
            log.warning("robots.txt zabrania: %s - pomijam", url)
            return None
        if self.count >= MAX_REQUESTS_PER_RUN:
            raise Blocked(f"Osiągnięto limit {MAX_REQUESTS_PER_RUN} requestów na przebieg")

        for attempt in range(1, MAX_RETRIES + 1):
            self._wait()
            self.count += 1
            try:
                r = self.s.get(url, timeout=TIMEOUT)
            except requests.RequestException as e:
                self._last = time.monotonic()
                log.warning("Błąd sieci (%s), próba %d/%d", e, attempt, MAX_RETRIES)
                time.sleep(20 * attempt)
                continue
            self._last = time.monotonic()

            if urlparse(r.url).netloc != p.netloc or not urlparse(r.url).path.startswith("/oferty"):
                raise Blocked(f"Przekierowanie na {r.url} (captcha / logowanie / zmiana struktury?)")
            if r.status_code == 200:
                self._strikes = 0
                if "captcha" in r.text[:5000].lower():
                    raise Blocked("Strona z captcha - zatrzymuję się.")
                return r.text
            if r.status_code == 403:
                raise Blocked("HTTP 403 - dostęp zablokowany. Nie obchodzę tego; zatrzymuję się.")
            if r.status_code == 429:
                self._strikes += 1
                if self._strikes >= 2:
                    raise Blocked("Powtarzające się HTTP 429 - zatrzymuję się.")
                ra = r.headers.get("Retry-After", "")
                wait = min(int(ra) if ra.isdigit() else 180, 900)
                log.warning("HTTP 429, czekam %ds", wait)
                time.sleep(wait)
                continue
            if 500 <= r.status_code < 600:
                wait = 30 * attempt
                log.warning("HTTP %d, czekam %ds", r.status_code, wait)
                time.sleep(wait)
                continue
            log.error("Nieoczekiwany HTTP %d dla %s", r.status_code, url)
            return None
        return None


# ----------------------------------------------------------------- PARSOWANIE
_ARTICLE_RE = re.compile(r"<article\b.*?</article>", re.S)
_LD_RE = re.compile(r"<script[^>]*application/ld\+json[^>]*>(.*?)</script>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_SVG_RE = re.compile(r"<svg\b.*?</svg>", re.S)

CONDITION_MAP = {
    "NewCondition": "nowe",
    "UsedCondition": "używane",
    "DamagedCondition": "uszkodzone",
    "RefurbishedCondition": "odnowione",
}
OFFER_TYPE_MAP = {
    "buy_now": "kup teraz",
    "classified": "ogłoszenie",
    "bidding": "licytacja (cena = bieżąca oferta, nie końcowa)",
    "allegro": "oferta z allegro.pl",
}


def _text(fragment):
    fragment = _SVG_RE.sub("", fragment)
    return re.sub(r"\s+", " ", htmllib.unescape(_TAG_RE.sub(" ", fragment))).strip()


def parse_ld(page_html):
    """JSON-LD ItemList -> {url: {'price': float|None, 'condition': str|None, 'name': str}}"""
    out = {}
    for body in _LD_RE.findall(page_html):
        try:
            data = json.loads(body)
        except ValueError:
            continue
        if not isinstance(data, dict) or data.get("@type") != "ItemList":
            continue
        for el in data.get("itemListElement") or []:
            item = el.get("item") or {}
            url = item.get("url")
            if not url:
                continue
            try:
                price = round(float((item.get("offers") or {}).get("price")), 2)
            except (TypeError, ValueError):
                price = None
            cond = (item.get("itemCondition") or "").rsplit("/", 1)[-1]
            out[url] = {"price": price, "condition": cond, "name": item.get("name")}
    return out


def normalize_url(url):
    url = htmllib.unescape(url).split("#")[0]
    if "allegro.pl/oferta/" in url and "allegrolokalnie" not in url:
        # https://allegro.pl/oferta/<slug>-<id>?... -> https://allegro.pl/oferta/<id> (jak w allegro_scraper.py)
        m = re.search(r"allegro\.pl/oferta/(?:[^?/]*-)?(\d{6,})", url)
        if m:
            return f"https://allegro.pl/oferta/{m.group(1)}", "Allegro"
        return url.split("?")[0], "Allegro"
    return url.split("?")[0], "Allegro Lokalnie"


def _html_price(article):
    m = re.search(r'ml-offer-price__dollars">([^<]*)<', article)
    if not m:
        return None
    whole = re.sub(r"[^\d]", "", htmllib.unescape(m.group(1)))
    c = re.search(r'ml-offer-price__cents">([^<]*)<', article)
    cents = re.sub(r"[^\d]", "", htmllib.unescape(c.group(1))) if c else ""
    if not whole:
        return None
    return float(f"{whole}.{cents}") if cents else float(whole)


def parse_article(article, ld):
    m = re.search(r'href="([^"]+)"[^>]*itemprop="url"', article) or re.search(r'itemprop="url"[^>]*href="([^"]+)"', article)
    if not m:
        return None
    raw_url = htmllib.unescape(m.group(1))
    meta = ld.get(raw_url, {})
    condition = CONDITION_MAP.get(meta.get("condition", ""), None)
    if SKIP_NEW and condition == "nowe":
        return None
    url, platform = normalize_url(raw_url)

    t = re.search(r"mlc-itembox__title[^>]*>(.*?)</h3>", article, re.S)
    title = _text(t.group(1)) if t else (meta.get("name") or "")
    price = meta.get("price") if meta.get("price") is not None else _html_price(article)

    loc = re.search(r'<address[^>]*>(.*?)</address>', article, re.S)
    location = _text(loc.group(1))[:100] if loc else None

    otype = (re.search(r"mlc-itembox__offer-type--([a-z_]+)", article) or [None, None])[1]
    lines = []
    if otype:
        lines.append("Typ oferty: " + OFFER_TYPE_MAP.get(otype, otype))
    if condition:
        lines.append("Stan: " + condition)
    for name_html, val_html in re.findall(
            r'ml-text-small">\s*(.*?)<span class="mlc-itembox__params__param__name">(.*?)</span>', article, re.S):
        name, val = _text(name_html).rstrip(":").strip(), _text(val_html)
        if name and val:
            lines.append(f"{name}: {val}")
    seller = None
    for li in re.findall(r'<li[^>]*class="ml-badges__badge[^>]*>(.*?)</li>', article, re.S):
        b = _text(li)
        if b.lower().startswith("sprzedający:"):
            seller = b
        elif b and not b.lower().startswith("stan:"):
            lines.append(b)  # np. "Często sprzedaje"
    if seller:
        lines.insert(1 if otype else 0, seller)
    flat = _text(article)
    if re.search(r"do negocjacji", flat, re.I):
        lines.append("Cena: do negocjacji")
    old = re.search(r'ml-lower-price__old-price"[^>]*>(.*?)</span>\s*</div>', article, re.S)
    if old:
        lines.append("Poprzednia cena: " + _text(old.group(1)))
    bids = re.search(r"(\d+\s+os\w+\s+licytuj\w+)", flat)
    if bids:
        lines.append(bids.group(1))
    left = re.search(r"(\d{1,2}:\d{2}:\d{2}|\d+\s*(?:dni|dzień|d)\b[^<]{0,20})\s+do końca", flat)
    if left:
        lines.append(f"Do końca licytacji: {left.group(1)}")

    return {
        "platform": platform,
        "url": url,
        "title": title,
        "price": price,
        "raw_description": "\n".join(lines),
        "seller_location": location,
    }


def parse_page(page_html):
    """-> (lista ogłoszeń, liczba pominiętych nowych, czy jest następna strona)."""
    if "data-offer-listing-form" not in page_html and "mlc-itembox" not in page_html:
        raise Blocked("Odpowiedź nie wygląda jak listing Allegro Lokalnie (captcha / zmiana struktury?)")
    ld = parse_ld(page_html)
    seen, out, skipped = set(), [], 0
    articles = _ARTICLE_RE.findall(page_html)
    for art in articles:
        p = parse_article(art, ld)
        if p is None:
            skipped += 1
            continue
        if p["url"] not in seen:
            seen.add(p["url"])
            out.append(p)
    return out, skipped, len(articles)


def has_next_page(page_html, page):
    return re.search(r"[?&;]page=%d(?!\d)" % (page + 1), page_html) is not None


# ----------------------------------------------------------------- DB
def connect(db_path):
    db_path = Path(db_path)
    if not db_path.exists():
        sys.exit(f"Brak bazy {db_path} - najpierw uruchom db_setup.py (albo podaj --db).")
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
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


def save(conn, parsed, dry_run):
    already = known_urls(conn, [p["url"] for p in parsed])
    new_count = sum(1 for p in parsed if p["url"] not in already)
    if dry_run:
        for p in parsed[:5]:
            log.info("DRY  [%s] %s | %s zł | %s | %s", p["platform"], p["title"][:50], p["price"],
                     p["seller_location"], p["url"])
    else:
        conn.executemany(UPSERT_SQL, parsed)
        conn.commit()
    return new_count


# ----------------------------------------------------------------- GŁÓWNA PĘTLA
def build_url(query, page):
    params = [("zrodlo", s) for s in SOURCES] + [("sort", SORT)]
    if page > 1:
        params.append(("page", page))
    return f"{HOST}/oferty/q/{quote(query, safe='')}?{urlencode(params)}"


def scrape_query(fetcher, conn, query, max_pages, incremental, dry_run):
    new_total = upd_total = 0
    for page in range(1, max_pages + 1):
        page_html = fetcher.get_html(build_url(query, page))
        if page_html is None:
            break
        parsed, skipped, n_art = parse_page(page_html)
        if n_art == 0:
            break
        new_count = save(conn, parsed, dry_run)
        new_total += new_count
        upd_total += len(parsed) - new_count
        log.info("[%s] strona %d: %d ogł. (%d nowych, pominięto %d nowych przedmiotów)",
                 query, page, len(parsed), new_count, skipped)
        if dry_run:
            break
        if incremental and new_count == 0:
            log.info("[%s] --incremental: nic nowego, kończę to zapytanie", query)
            break
        if not has_next_page(page_html, page):
            break
    return new_total, upd_total


def run_offline(conn, target, dry_run):
    path = Path(target)
    files = sorted(path.glob("*.html")) if path.is_dir() else [path]
    total_new = total_upd = 0
    for f in files:
        try:
            parsed, skipped, n_art = parse_page(f.read_text(encoding="utf-8", errors="replace"))
        except Blocked as e:
            log.error("%s: %s", f.name, e)
            continue
        new_count = save(conn, parsed, dry_run)
        total_new += new_count
        total_upd += len(parsed) - new_count
        log.info("%s: %d kafelków, zapisano %d (%d nowych), pominięto %d nowych przedmiotów",
                 f.name, n_art, len(parsed), new_count, skipped)
    return total_new, total_upd


def main():
    ap = argparse.ArgumentParser(description="Allegro Lokalnie -> SQLite scraper")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("-q", "--query", action="append", help="własne zapytanie (można wielokrotnie)")
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES_PER_QUERY)
    ap.add_argument("--incremental", action="store_true")
    ap.add_argument("--dry-run", action="store_true", help="bez zapisu do bazy; online: tylko 1 request")
    ap.add_argument("--from-html", metavar="PLIK_LUB_KATALOG", help="parsuj zapisane strony, bez requestów")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    conn = connect(args.db)

    if args.from_html:
        n, u = run_offline(conn, args.from_html, args.dry_run)
        log.info("Koniec (offline): %d nowych, %d zaktualizowanych", n, u)
        return

    if not CONTACT:
        log.warning("Brak ALLEGRO_SCRAPER_CONTACT / OLX_SCRAPER_CONTACT - User-Agent bez kontaktu.")
    queries = args.query or QUERIES
    if args.dry_run:
        queries = queries[:1]

    fetcher = Fetcher()
    total_new = total_upd = 0
    try:
        fetcher.load_robots()
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
