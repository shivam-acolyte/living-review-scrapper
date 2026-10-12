#!/usr/bin/env python3
"""
scrape_target.py — Targeted Google Maps Review Scraper

Reads house_ids from  target.txt  (one per line, # = comment),
looks up the corresponding place_id in property_business_ids.csv,
then scrolls INDEFINITELY until no new reviews appear for
STALL_ROUNDS consecutive scrolls (i.e., truly fetches ALL reviews).

Outputs are appended to:
  - property_reviews.csv
  - property_reviews.sql

And seeded into PostgreSQL (or SQLite via DB_TYPE=sqlite in .env).

Usage:
  1. Add house_ids to  target.txt  (one per line)
  2. python scrape_target.py
"""

import os, sys, csv, json, time, re, gc, signal, logging, hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("scrape_target")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR        = Path(__file__).resolve().parent
TARGET_FILE     = BASE_DIR / "target.txt"
INPUT_CSV       = BASE_DIR / "property_business_ids.csv"
OUTPUT_CSV      = BASE_DIR / "property_reviews.csv"
OUTPUT_SQL      = BASE_DIR / "property_reviews.sql"
CACHE_FILE      = BASE_DIR / "reviews_cache_done.json"   # shared with pipeline
TARGET_CACHE    = BASE_DIR / "target_done.json"           # tracks THIS script's done ids
NO_REVIEWS_FILE = BASE_DIR / "no_reviews.json"            # house_ids confirmed to have 0 Google reviews

# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------
SEED_BATCH_SIZE = 100    # flush to disk/DB every N reviews
SCROLL_SLEEP    = 1.2    # seconds between scroll steps
NAV_SLEEP       = 2.0    # seconds after page navigation
BETWEEN_SLEEP   = 1.0    # seconds between properties
STALL_ROUNDS    = 5      # stop scrolling after this many rounds with no new reviews

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
stop_requested = False

def _handle_signal(signum, frame):
    global stop_requested
    logger.warning("\n[!] Stop requested — finishing current property…")
    stop_requested = True

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ---------------------------------------------------------------------------
# .env loader
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
                    k = k.strip(); v = v.strip().strip('"').strip("'")
                    if k and k not in os.environ:
                        os.environ[k] = v
            break

load_env_file()
REVIEWS_TABLE = os.environ.get("REVIEWS_TABLE", "new_house_reviews")

# ---------------------------------------------------------------------------
# Database
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
        _db_conn = sqlite3.connect(
            str(BASE_DIR / os.environ.get("SQLITE_PATH", "seeded_properties.db"))
        )
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
            host     = os.environ.get("DB_HOST", "localhost"),
            port     = int(os.environ.get("DB_PORT", 5432)),
            dbname   = os.environ.get("DB_NAME", "postgres"),
            user     = os.environ.get("DB_USER", "postgres"),
            password = os.environ.get("DB_PASSWORD", ""),
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


def seed_reviews_to_db(batch: list) -> int:
    if not batch:
        return 0

    # Python-side dedup
    seen, unique = set(), []
    for r in batch:
        key = hashlib.md5(
            f"{r['house_id']}|{(r.get('author') or 'Anonymous').lower()}|"
            f"{(r.get('comment') or '')[:255].lower()}".encode()
        ).hexdigest()
        if key not in seen:
            seen.add(key)
            unique.append(r)
    batch = unique
    if not batch:
        return 0

    conn = get_db_conn()
    if conn is None:
        logger.warning("No DB connection — skipping seed.")
        return 0

    now_ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    db_type = os.environ.get("DB_TYPE", "postgres").lower()

    rows = []
    for r in batch:
        rating   = r.get("rating")
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
# CSV / SQL file helpers  (shared schema with scrape_reviews_pipeline.py)
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
            rid  = start_id + i
            hid  = r["house_id"]
            auth = (r.get("author") or "Anonymous").replace("'","''")
            rating = r.get("rating")
            rv   = f"{float(rating):.2f}" if rating is not None else "5.00"
            sv   = str(int(round(float(rating)))) if rating is not None else "5"
            cmt  = (r.get("comment") or "").replace("'","''")
            cat  = r.get("created_at") or now_ts
            f.write(
                f"INSERT INTO {REVIEWS_TABLE} ("
                "id,house_id,user_id,reviewer_name,reviewer_email,rating,star_num,comment,"
                "school_name,unit_name,status,source,created_at,updated_at,deleted_at,translated_comment) VALUES ("
                f"{rid},{hid},NULL,'{auth}',NULL,{rv},{sv},'{cmt}',"
                f"NULL,NULL,'pending','google_review','{cat}','{now_ts}',NULL,NULL);\n"
            )

# ---------------------------------------------------------------------------
# Done-set helpers  (target_done.json tracks ids done by THIS script)
# ---------------------------------------------------------------------------
def load_target_done() -> set:
    if TARGET_CACHE.exists():
        try:
            with open(TARGET_CACHE, encoding="utf-8") as f:
                return set(str(x) for x in json.load(f).get("done", []))
        except Exception as e:
            logger.warning("target_done load error: %s", e)
    return set()

def save_target_done(done: set):
    tmp = str(TARGET_CACHE) + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump({"done": sorted(done)}, f)
        os.replace(tmp, str(TARGET_CACHE))
    except Exception as e:
        logger.error("target_done save error: %s", e)

# ---------------------------------------------------------------------------
# No-reviews tracker  (no_reviews.json — permanently skip 0-review properties)
# ---------------------------------------------------------------------------
def load_no_reviews() -> dict:
    """
    Returns a dict: { house_id_str -> {title, place_id, recorded_at} }
    """
    if NO_REVIEWS_FILE.exists():
        try:
            with open(NO_REVIEWS_FILE, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("no_reviews load error: %s", e)
    return {}

def save_no_reviews(no_rev: dict):
    tmp = str(NO_REVIEWS_FILE) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(no_rev, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(NO_REVIEWS_FILE))
    except Exception as e:
        logger.error("no_reviews save error: %s", e)

def record_no_review(no_rev: dict, prop: dict):
    """Add a property to the no-reviews registry and persist immediately."""
    hid_str = str(prop["house_id"])
    no_rev[hid_str] = {
        "house_id":    prop["house_id"],
        "title":       prop.get("title", ""),
        "place_id":    prop.get("place_id", ""),
        "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"),
    }
    save_no_reviews(no_rev)
    logger.info(
        "  ∅ Recorded as no-review property: house_id=%s ('%s') → no_reviews.json",
        hid_str, prop.get("title", "?")
    )

# ---------------------------------------------------------------------------
# Relative-date parser
# ---------------------------------------------------------------------------
def parse_relative_date(s: str) -> str:
    now = datetime.now(timezone.utc)
    if not s:
        return now.strftime("%Y-%m-%d %H:%M:%S+00")
    s = s.lower()
    m = re.search(r"(\d+)\s*(day|week|month|year|hour|minute)s?\s*ago", s)
    if m:
        v, u = int(m.group(1)), m.group(2)
        delta = {
            "minute": timedelta(minutes=v), "hour":  timedelta(hours=v),
            "day":    timedelta(days=v),     "week":  timedelta(weeks=v),
            "month":  timedelta(days=v*30),  "year":  timedelta(days=v*365),
        }.get(u, timedelta(0))
        return (now - delta).strftime("%Y-%m-%d %H:%M:%S+00")
    for phrase, days in [("a month",30),("a year",365),("a week",7),
                          ("yesterday",1),("a day ago",1)]:
        if phrase in s:
            return (now - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S+00")
    return now.strftime("%Y-%m-%d %H:%M:%S+00")

# ---------------------------------------------------------------------------
# Core scraper — NO max scroll limit, stops when review count stalls
# ---------------------------------------------------------------------------
def scrape_all_reviews(page, place_id: str, title: str) -> list:
    """
    Navigates to the Google Maps listing and scrolls until no new reviews
    appear for STALL_ROUNDS consecutive scroll rounds.
    Returns a list of dicts: {author, rating, text, date}
    """
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
    logger.info("    → Navigating to: %s", url)
    try:
        page.goto(url, wait_until="networkidle", timeout=40000)
    except Exception:
        page.goto(url, wait_until="domcontentloaded", timeout=40000)
    time.sleep(NAV_SLEEP)

    # Click the "Reviews" tab
    page.evaluate('''() => {
        const btns = document.querySelectorAll(
            'button[role="tab"], button.GQjSyb, .F7nice, button.hh2c6'
        );
        for (const b of btns) {
            const txt = (b.getAttribute("aria-label") || b.textContent || "").toLowerCase();
            if (txt.includes("review")) { b.click(); return; }
        }
    }''')
    time.sleep(1.8)

    # Dismiss cookie/consent banners
    page.evaluate('''() => {
        for (const b of document.querySelectorAll("button"))
            if ((b.textContent || "").toLowerCase().includes("accept all")
             || (b.textContent || "").toLowerCase().includes("agree")
             || (b.textContent || "").toLowerCase().includes("dismiss"))
                { b.click(); return; }
    }''')
    time.sleep(0.5)

    collected   = {}   # key=(author, text[:40]) → review dict
    stall_count = 0
    scroll_round = 0

    logger.info("    → Scrolling to collect ALL reviews (stall threshold: %d rounds)…", STALL_ROUNDS)

    while stall_count < STALL_ROUNDS:
        if stop_requested:
            break

        scroll_round += 1

        # Expand "See more" buttons
        page.evaluate('''() => {
            document.querySelectorAll("button.w8nwRe, button[aria-label*='See more']")
                .forEach(b => { try { b.click(); } catch(e) {} });
        }''')
        time.sleep(0.3)

        # Extract all visible review cards
        items = page.evaluate('''() => {
            return Array.from(document.querySelectorAll("div.jftiEf")).map(card => {
                const author = card.querySelector(".d4r55")?.textContent?.trim() || "Anonymous";
                const text   = card.querySelector(".wiI7pd")?.textContent?.trim() || "";
                const rEl    = card.querySelector(".kvMYJc, span[role='img'][aria-label*='star']");
                let rating   = null;
                if (rEl) {
                    const m = (rEl.getAttribute("aria-label") || "").match(/([0-9]+(\.[0-9]+)?)/);
                    if (m) rating = parseFloat(m[1]);
                }
                const date = card.querySelector(".rsqaWe")?.textContent?.trim() || "";
                return { author, rating, text, date };
            });
        }''')

        before = len(collected)
        for it in items:
            key = (it["author"], it["text"][:40])
            if key not in collected:
                collected[key] = it

        new_this_round = len(collected) - before

        if new_this_round == 0:
            stall_count += 1
            logger.debug(
                "    round %d — no new reviews (stall %d/%d), total so far: %d",
                scroll_round, stall_count, STALL_ROUNDS, len(collected)
            )
        else:
            stall_count = 0   # reset stall counter when we find new reviews
            logger.info(
                "    round %d — +%d new reviews, total: %d",
                scroll_round, new_this_round, len(collected)
            )

        # Scroll down the reviews panel
        page.evaluate('''() => {
            const selectors = [
                ".m6QErb.DxyBCb", ".m6QErb", "div[role='main']",
                ".section-scrollbox", "[data-scroll-offset]"
            ];
            for (const sel of selectors) {
                const els = document.querySelectorAll(sel);
                for (const el of els) {
                    if (el.scrollHeight > el.clientHeight) {
                        el.scrollTop += 1200;
                    }
                }
            }
        }''')
        time.sleep(SCROLL_SLEEP)

    logger.info(
        "    → Stopped after %d scroll rounds. Total reviews collected: %d",
        scroll_round, len(collected)
    )
    return list(collected.values())


# ---------------------------------------------------------------------------
# Input readers
# ---------------------------------------------------------------------------
def read_target_ids() -> list[int]:
    """Read house_ids from target.txt, skipping comments and blanks."""
    if not TARGET_FILE.exists():
        logger.error("target.txt not found at %s", TARGET_FILE)
        sys.exit(1)
    ids = []
    with open(TARGET_FILE, encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            try:
                ids.append(int(line))
            except ValueError:
                logger.warning("Skipping non-integer line in target.txt: %r", line)
    return ids


def load_property_index() -> dict:
    """
    Returns a dict keyed by house_id (int) mapping to the full property row
    from property_business_ids.csv.
    """
    index = {}
    if not INPUT_CSV.exists():
        logger.error("property_business_ids.csv not found at %s", INPUT_CSV)
        return index
    with open(INPUT_CSV, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                hid = int(row.get("house_id", 0))
            except (ValueError, TypeError):
                continue
            if hid:
                index[hid] = row
    return index


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 64)
    logger.info("scrape_target.py — Fetch ALL Reviews for Targeted Houses")
    logger.info("=" * 64)

    # --- Read targets ---
    target_ids = read_target_ids()
    if not target_ids:
        logger.error("target.txt is empty — add house_ids and re-run.")
        sys.exit(0)
    logger.info("Target house_ids (%d): %s", len(target_ids), target_ids)

    # --- Build lookup from CSV ---
    prop_index = load_property_index()
    logger.info("Loaded property index: %d entries", len(prop_index))

    # --- Resolve targets ---
    resolved = []
    for hid in target_ids:
        row = prop_index.get(hid)
        if not row:
            logger.warning("house_id %s not found in property_business_ids.csv — skipping.", hid)
            continue
        pid = (row.get("place_id") or "").strip()
        if not pid or pid in ("N/A", ""):
            logger.warning("house_id %s has no place_id — skipping.", hid)
            continue
        resolved.append({
            "house_id":  hid,
            "title":     row.get("title", "?"),
            "place_id":  pid,
            "review_count": row.get("review_count", "?"),
        })
    logger.info("Resolved to %d properties with valid place_id.", len(resolved))

    if not resolved:
        logger.error("No valid properties to scrape.")
        sys.exit(0)

    # --- Track what this script has already done ---
    done_ids = load_target_done()
    no_rev   = load_no_reviews()

    # Skip properties already confirmed to have no reviews
    skipped_no_rev = [p for p in resolved if str(p["house_id"]) in no_rev]
    if skipped_no_rev:
        logger.info(
            "Skipping %d properties already confirmed to have 0 Google reviews "
            "(recorded in no_reviews.json):",
            len(skipped_no_rev)
        )
        for p in skipped_no_rev:
            logger.info(
                "  ∅ house_id=%-10s  %s  (recorded: %s)",
                p["house_id"], p["title"],
                no_rev[str(p["house_id"])].get("recorded_at", "?"),
            )

    pending = [
        p for p in resolved
        if str(p["house_id"]) not in done_ids
        and str(p["house_id"]) not in no_rev
    ]
    logger.info(
        "Already done: %d  |  No-review (skipped): %d  |  Pending: %d",
        len(done_ids), len(no_rev), len(pending)
    )

    if not pending:
        logger.info("All targeted properties are already scraped — nothing to do.")
        return

    # --- Scrape ---
    total_scraped = 0
    rolling_batch = []
    batch_no      = 0

    with sync_playwright() as pw:
        browser = pw.firefox.launch(headless=True)
        ctx = browser.new_context(
            viewport   = {"width": 1280, "height": 900},
            user_agent = (
                "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
                "Gecko/20100101 Firefox/128.0"
            ),
            locale = "en-US",
        )
        page = ctx.new_page()

        # Dismiss cookie banner once at start
        try:
            page.goto("https://www.google.com/maps?hl=en",
                      wait_until="networkidle", timeout=30000)
            time.sleep(1.5)
            page.evaluate('''() => {
                for (const b of document.querySelectorAll("button")) {
                    const t = (b.textContent || "").toLowerCase();
                    if (t.includes("accept all") || t.includes("agree")
                     || t.includes("reject all") || t.includes("dismiss")) {
                        b.click(); return;
                    }
                }
            }''')
            time.sleep(1)
        except Exception as e:
            logger.warning("Cookie banner dismissal failed (non-fatal): %s", e)

        try:
            for idx, prop in enumerate(pending, start=1):
                if stop_requested:
                    logger.info("Stop requested — exiting.")
                    break

                hid   = prop["house_id"]
                title = prop["title"]
                pid   = prop["place_id"]
                rc    = prop["review_count"]

                logger.info(
                    "[%d/%d] House %s  '%s'  (expected reviews: %s)",
                    idx, len(pending), hid, title, rc
                )

                try:
                    raw = scrape_all_reviews(page, pid, title)
                    logger.info("  → Total extracted: %d reviews", len(raw))

                    if len(raw) == 0:
                        # Confirmed zero reviews — record permanently so future
                        # runs skip this property without re-scanning.
                        record_no_review(no_rev, prop)
                    else:
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
                save_target_done(done_ids)

                # Flush every SEED_BATCH_SIZE reviews to disk + DB
                if len(rolling_batch) >= SEED_BATCH_SIZE:
                    batch_no += 1
                    logger.info(
                        "  ► Batch #%d (%d reviews) → files + DB …",
                        batch_no, len(rolling_batch)
                    )
                    append_reviews_to_files(rolling_batch)
                    seed_reviews_to_db(rolling_batch)
                    rolling_batch.clear()
                    gc.collect()
                    logger.info(
                        "  ✓ Batch #%d done. Total scraped this run: %d",
                        batch_no, total_scraped
                    )

                time.sleep(BETWEEN_SLEEP)

        finally:
            if rolling_batch:
                batch_no += 1
                logger.info(
                    "  ► Final batch #%d (%d reviews) → files + DB …",
                    batch_no, len(rolling_batch)
                )
                append_reviews_to_files(rolling_batch)
                seed_reviews_to_db(rolling_batch)
                rolling_batch.clear()
                gc.collect()

            page.close()
            ctx.close()
            browser.close()

    close_db()
    logger.info("=" * 64)
    logger.info("Done!  %d batches | %d reviews | %d properties done",
                batch_no, total_scraped, len(done_ids))
    logger.info("No-review properties recorded: %d  → no_reviews.json", len(no_rev))
    logger.info("CSV → %s", OUTPUT_CSV)
    logger.info("SQL → %s", OUTPUT_SQL)
    logger.info("=" * 64)


if __name__ == "__main__":
    main()
