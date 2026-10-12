#!/usr/bin/env python3
"""
Google Maps Property Reviews Scraper — Memory-Efficient Streaming Pipeline.

Key design principles:
  - Zero in-memory accumulation: reviews streamed to CSV/SQL appends per-property
  - Rolling batch of exactly SEED_BATCH_SIZE reviews held in RAM; flushed & discarded
  - Cache tracks only completed house_ids (flat set), not full review payloads
  - Every 100 scraped reviews committed directly to PostgreSQL and RAM is freed
  - Legacy reviews_cache.json migrated once at startup, then only ids kept
  - Playwright browser reused across all properties (no restart per property)

Schema: house_reviews
  id, house_id, user_id, reviewer_name, reviewer_email, rating, star_num,
  comment, school_name, unit_name, status, source, created_at, updated_at,
  deleted_at, translated_comment
"""

import os, sys, csv, json, time, re, gc, signal, logging, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("review_scraper")

BASE_DIR   = Path(__file__).resolve().parent
INPUT_CSV  = BASE_DIR / "property_business_ids.csv"
OUTPUT_CSV = BASE_DIR / "property_reviews.csv"
OUTPUT_SQL = BASE_DIR / "property_reviews.sql"
CACHE_FILE = BASE_DIR / "reviews_cache_done.json"   # lightweight: only done ids

SEED_BATCH_SIZE = 100
MAX_SCROLLS     = 5
SCROLL_SLEEP    = 1.0
NAV_SLEEP       = 1.2
BETWEEN_SLEEP   = 0.5

# ---------------------------------------------------------------------------
stop_requested = False
def handle_sigint(signum, frame):
    global stop_requested
    logger.warning("\n[!] Graceful stop requested. Finishing current property.")
    stop_requested = True
signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

# ---------------------------------------------------------------------------
def load_env_file():
    for cand in [BASE_DIR / ".env", BASE_DIR.parent / ".env"]:
        if cand.exists():
            with open(cand, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip().strip('"').strip("'")
                    if k and k not in os.environ:
                        os.environ[k] = v
            break
load_env_file()

REVIEWS_TABLE = os.environ.get("REVIEWS_TABLE", "new_house_reviews")

# ---------------------------------------------------------------------------
_db_conn = None
def get_db_conn():
    global _db_conn
    if _db_conn is not None:
        try:
            _db_conn.cursor().execute("SELECT 1")
            return _db_conn
        except Exception:
            _db_conn = None

    db_type = os.environ.get("DB_TYPE", "postgres").lower()
    if db_type == "sqlite":
        import sqlite3
        _db_conn = sqlite3.connect(str(BASE_DIR / os.environ.get("SQLITE_PATH", "seeded_properties.db")))
        return _db_conn
    try:
        import psycopg2
    except ImportError:
        logger.error("psycopg2 not installed — pip install psycopg2-binary")
        return None
    url = os.environ.get("DATABASE_URL")
    if url:
        _db_conn = psycopg2.connect(url)
    else:
        _db_conn = psycopg2.connect(
            host=os.environ.get("DB_HOST", "localhost"),
            port=int(os.environ.get("DB_PORT", 5432)),
            dbname=os.environ.get("DB_NAME", "postgres"),
            user=os.environ.get("DB_USER", "postgres"),
            password=os.environ.get("DB_PASSWORD", ""),
            connect_timeout=10,
        )
    _db_conn.autocommit = False
    return _db_conn

def close_db():
    global _db_conn
    if _db_conn:
        try: _db_conn.close()
        except: pass
        _db_conn = None

# ---------------------------------------------------------------------------
def seed_reviews_to_db(batch: list) -> int:
    if not batch:
        return 0
    # Python-side dedup: drop rows with identical (house_id, reviewer_name, comment[:255])
    seen = set()
    unique_batch = []
    for r in batch:
        key = hashlib.md5(
            f"{r['house_id']}|{(r.get('author') or 'Anonymous').lower()}|{(r.get('comment') or '')[:255].lower()}".encode()
        ).hexdigest()
        if key not in seen:
            seen.add(key)
            unique_batch.append(r)
    batch = unique_batch
    if not batch:
        return 0
    conn = get_db_conn()
    if conn is None:
        logger.warning("No DB connection — skipping seed for this batch.")
        return 0
    now_ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    db_type = os.environ.get("DB_TYPE", "postgres").lower()
    rows = []
    for r in batch:
        rating = r.get("rating")
        rating_v = round(float(rating), 2) if rating is not None else 5.00
        star_v   = int(round(float(rating))) if rating is not None else 5
        rows.append((
            r["house_id"], None, (r.get("author") or "Anonymous"), None,
            rating_v, star_v,
            (r.get("comment") or ""),
            None, None, "pending", "google_review",
            (r.get("created_at") or now_ts), now_ts, None, None,
        ))
    if db_type == "postgres":
        sql = (
            f"INSERT INTO {REVIEWS_TABLE} "
            "(house_id,user_id,reviewer_name,reviewer_email,rating,star_num,comment,"
            "school_name,unit_name,status,source,created_at,updated_at,deleted_at,translated_comment) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)\n"
            "ON CONFLICT (house_id, reviewer_name, LEFT(comment, 255))\n"
            "DO NOTHING"
        )
    else:
        sql = (
            f"INSERT OR IGNORE INTO {REVIEWS_TABLE} "
            "(house_id,user_id,reviewer_name,reviewer_email,rating,star_num,comment,"
            "school_name,unit_name,status,source,created_at,updated_at,deleted_at,translated_comment) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
    try:
        cur = conn.cursor()
        if db_type == "postgres":
            import psycopg2.extras
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        else:
            cur.executemany(sql, rows)
        conn.commit()
        logger.info("  ✓ DB: %d reviews → %s", len(rows), REVIEWS_TABLE)
        return len(rows)
    except Exception as e:
        logger.error("  ✗ DB commit failed: %s", e)
        try: conn.rollback()
        except: pass
        return 0

# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    "id","house_id","user_id","reviewer_name","reviewer_email",
    "rating","star_num","comment","school_name","unit_name",
    "status","source","created_at","updated_at","deleted_at","translated_comment",
]

def ensure_csv_header():
    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()

def ensure_sql_header():
    if not OUTPUT_SQL.exists():
        with open(OUTPUT_SQL, "w", encoding="utf-8") as f:
            f.write(f"-- Google Maps Reviews  |  table: {REVIEWS_TABLE}\n\n")

def _count_csv_rows() -> int:
    if not OUTPUT_CSV.exists():
        return 0
    with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)

def append_reviews_to_files(batch: list):
    if not batch:
        return
    now_ts   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    ensure_csv_header()
    ensure_sql_header()
    start_id = _count_csv_rows() + 1

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        for i, r in enumerate(batch):
            rating = r.get("rating")
            w.writerow({
                "id":                start_id + i,
                "house_id":          r["house_id"],
                "user_id":           "",
                "reviewer_name":     r.get("author") or "Anonymous",
                "reviewer_email":    "",
                "rating":            f"{float(rating):.2f}" if rating is not None else "5.00",
                "star_num":          int(round(float(rating))) if rating is not None else 5,
                "comment":           r.get("comment") or "",
                "school_name":       "",
                "unit_name":         "",
                "status":            "pending",
                "source":            "google_review",
                "created_at":        r.get("created_at") or now_ts,
                "updated_at":        now_ts,
                "deleted_at":        "",
                "translated_comment":"",
            })

    with open(OUTPUT_SQL, "a", encoding="utf-8") as f:
        for i, r in enumerate(batch):
            rid    = start_id + i
            hid    = r["house_id"]
            auth   = (r.get("author") or "Anonymous").replace("'","''")
            rating = r.get("rating")
            rv     = f"{float(rating):.2f}" if rating is not None else "5.00"
            sv     = str(int(round(float(rating)))) if rating is not None else "5"
            cmt    = (r.get("comment") or "").replace("'","''")
            cat    = r.get("created_at") or now_ts
            f.write(
                f"INSERT INTO {REVIEWS_TABLE} ("
                "id,house_id,user_id,reviewer_name,reviewer_email,rating,star_num,comment,"
                "school_name,unit_name,status,source,created_at,updated_at,deleted_at,translated_comment) VALUES ("
                f"{rid},{hid},NULL,'{auth}',NULL,{rv},{sv},'{cmt}',"
                f"NULL,NULL,'pending','google_review','{cat}','{now_ts}',NULL,NULL);\n"
            )

# ---------------------------------------------------------------------------
def load_done_set() -> set:
    if CACHE_FILE.exists():
        try:
            with open(CACHE_FILE, encoding="utf-8") as f:
                return set(str(x) for x in json.load(f).get("done", []))
        except Exception as e:
            logger.warning("Cache load error: %s", e)
    return set()

def save_done_set(done: set):
    tmp = str(CACHE_FILE) + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump({"done": sorted(done)}, f)
        os.replace(tmp, str(CACHE_FILE))
    except Exception as e:
        logger.error("Cache save error: %s", e)

# ---------------------------------------------------------------------------
LEGACY_CACHE = BASE_DIR / "reviews_cache.json"

def migrate_legacy_cache() -> set:
    if not LEGACY_CACHE.exists():
        return set()
    flag = BASE_DIR / ".legacy_cache_migrated"
    if flag.exists():
        try:
            with open(LEGACY_CACHE, encoding="utf-8") as f:
                return set(str(k) for k in json.load(f).keys())
        except Exception:
            return set()
    logger.info("Migrating legacy reviews_cache.json (one-time) …")
    try:
        with open(LEGACY_CACHE, encoding="utf-8") as f:
            old = json.load(f)
    except Exception as e:
        logger.error("Failed reading legacy cache: %s", e)
        return set()
    done_ids = set()
    batch, total = [], 0
    for hid_str, rev_list in old.items():
        done_ids.add(str(hid_str))
        for r in (rev_list or []):
            batch.append({
                "house_id":   int(hid_str),
                "author":     r.get("author"),
                "rating":     r.get("rating"),
                "comment":    r.get("text"),
                "created_at": parse_relative_date(r.get("date")),
            })
            if len(batch) >= SEED_BATCH_SIZE:
                append_reviews_to_files(batch)
                seed_reviews_to_db(batch)
                total += len(batch)
                batch.clear()
                gc.collect()
    if batch:
        append_reviews_to_files(batch)
        seed_reviews_to_db(batch)
        total += len(batch)
        batch.clear()
        gc.collect()
    logger.info("Migration done: %d properties, %d reviews.", len(done_ids), total)
    flag.touch()
    return done_ids

# ---------------------------------------------------------------------------
def parse_relative_date(s):
    now = datetime.now(timezone.utc)
    if not s:
        return now.strftime("%Y-%m-%d %H:%M:%S+00")
    s = s.lower()
    m = re.search(r"(\d+)\s*(day|week|month|year|hour|minute)s?\s*ago", s)
    if m:
        v, u = int(m.group(1)), m.group(2)
        delta = {"minute": timedelta(minutes=v), "hour": timedelta(hours=v),
                 "day": timedelta(days=v), "week": timedelta(weeks=v),
                 "month": timedelta(days=v*30), "year": timedelta(days=v*365)}.get(u, timedelta(0))
        return (now - delta).strftime("%Y-%m-%d %H:%M:%S+00")
    for phrase, days in [("a month",30),("a year",365),("a week",7),("yesterday",1),("a day ago",1)]:
        if phrase in s:
            return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00")
    return now.strftime("%Y-%m-%d %H:%M:%S+00")

# ---------------------------------------------------------------------------
def scrape_property_reviews(page, place_id: str) -> list:
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
    try:
        page.goto(url, wait_until="networkidle", timeout=30000)
    except Exception:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    time.sleep(NAV_SLEEP)

    page.evaluate('''() => {
        const btns = document.querySelectorAll('button[role="tab"],button.GQjSyb,.F7nice,button.hh2c6');
        for (const b of btns) {
            const txt = (b.getAttribute("aria-label")||b.textContent||"").toLowerCase();
            if (txt.includes("review")) { b.click(); return; }
        }
    }''')
    time.sleep(1.5)

    page.evaluate('''() => {
        for (const b of document.querySelectorAll("button"))
            if ((b.textContent||"").includes("Dismiss")) { b.click(); return; }
    }''')

    collected = {}
    for _ in range(MAX_SCROLLS):
        page.evaluate('''() => {
            document.querySelectorAll("button.w8nwRe,button[aria-label*='See more']")
                .forEach(b => { try { b.click(); } catch(e) {} });
        }''')
        time.sleep(0.3)

        items = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("div.jftiEf")).map(card => {
                const author = card.querySelector(".d4r55")?.textContent?.trim()||"Anonymous";
                const text   = card.querySelector(".wiI7pd")?.textContent?.trim()||"";
                const rEl    = card.querySelector(".kvMYJc,span[role='img'][aria-label*='star']");
                let rating   = null;
                if (rEl) {
                    const m = (rEl.getAttribute("aria-label")||"").match(/([0-9]+(\.[0-9]+)?)/);
                    if (m) rating = parseFloat(m[1]);
                }
                const date = card.querySelector(".rsqaWe")?.textContent?.trim()||"";
                return { author, rating, text, date };
            });
        }''')

        for it in items:
            key = (it["author"], it["text"][:40])
            if key not in collected:
                collected[key] = it

        page.evaluate('''() => {
            const cs = document.querySelectorAll(".m6QErb.DxyBCb,.m6QErb,div[role='main']");
            for (const c of cs) if (c.scrollHeight>c.clientHeight) c.scrollTop += 900;
        }''')
        time.sleep(SCROLL_SLEEP)

    return list(collected.values())

# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Memory-Efficient Review Scraper  |  batch=%d", SEED_BATCH_SIZE)
    logger.info("=" * 60)

    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        all_props = [
            p for p in csv.DictReader(f)
            if p.get("place_id") and p.get("place_id") not in ("", "N/A")
        ]
    logger.info("Properties with valid place_id: %d", len(all_props))

    done_ids  = migrate_legacy_cache()
    done_ids |= load_done_set()
    logger.info("Already scraped:    %d properties", len(done_ids))

    pending = [p for p in all_props if str(p["house_id"]) not in done_ids]
    logger.info("Pending properties: %d", len(pending))

    if not pending:
        logger.info("Nothing to scrape — all done!")
        return

    total_scraped = 0
    rolling_batch = []
    batch_no      = 0

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
            locale="en-US",
        )
        page = ctx.new_page()

        try:
            for idx, prop in enumerate(pending, start=1):
                if stop_requested:
                    logger.info("Stop requested — exiting.")
                    break

                hid   = prop["house_id"]
                title = prop.get("title", "?")
                pid   = prop["place_id"]
                rc    = prop.get("review_count", "?")
                logger.info("[%d/%d] House %s  '%s'  (expected reviews: %s)", idx, len(pending), hid, title, rc)

                try:
                    raw = scrape_property_reviews(page, pid)
                    logger.info("  → scraped %d reviews", len(raw))
                    for r in raw:
                        rolling_batch.append({
                            "house_id":   int(hid),
                            "author":     r.get("author"),
                            "rating":     r.get("rating"),
                            "comment":    r.get("text"),
                            "created_at": parse_relative_date(r.get("date")),
                        })
                    total_scraped += len(raw)
                    del raw
                except Exception as e:
                    logger.error("  ✗ Error scraping %s (%s): %s", hid, title, e)

                done_ids.add(str(hid))
                save_done_set(done_ids)

                # Flush every SEED_BATCH_SIZE reviews
                if len(rolling_batch) >= SEED_BATCH_SIZE:
                    batch_no += 1
                    logger.info("  ► Batch #%d (%d reviews) → files + DB …", batch_no, len(rolling_batch))
                    append_reviews_to_files(rolling_batch)
                    seed_reviews_to_db(rolling_batch)
                    rolling_batch.clear()
                    gc.collect()
                    logger.info("  ✓ Batch #%d done. Total scraped this run: %d", batch_no, total_scraped)

                time.sleep(BETWEEN_SLEEP)

        finally:
            if rolling_batch:
                batch_no += 1
                logger.info("  ► Final batch #%d (%d reviews) → files + DB …", batch_no, len(rolling_batch))
                append_reviews_to_files(rolling_batch)
                seed_reviews_to_db(rolling_batch)
                rolling_batch.clear()
                gc.collect()
            page.close()
            ctx.close()
            browser.close()

    close_db()
    logger.info("=" * 60)
    logger.info("Done!  %d batches | %d reviews this run | %d properties total", batch_no, total_scraped, len(done_ids))
    logger.info("CSV: %s", OUTPUT_CSV)
    logger.info("SQL: %s", OUTPUT_SQL)
    logger.info("=" * 60)

if __name__ == "__main__":
    main()
