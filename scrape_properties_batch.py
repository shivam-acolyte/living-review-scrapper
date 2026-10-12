#!/usr/bin/env python3
"""
Scrapes Google Maps reviews for the 16 requested properties using
robust DOM extraction logic from google-maps-scraper, exports to
JSON, CSV, and SQL, and seeds into PostgreSQL and SQLite.
"""

import os
import sys
import csv
import json
import time
import re
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta
from pathlib import Path
from playwright.sync_api import sync_playwright

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("batch16_scraper")

CURRENT_DIR = Path(__file__).resolve().parent
CACHE_FILE = str(CURRENT_DIR / "reviews_batch_16.json")
OUTPUT_CSV = str(CURRENT_DIR / "reviews_batch_16.csv")
OUTPUT_SQL = str(CURRENT_DIR / "reviews_batch_16.sql")
SQLITE_PATH = str(CURRENT_DIR / "seeded_properties.db")

TARGET_PROPERTIES = [
    {
        "house_id": 1577194,
        "title": "Canvas The Old Fire Station",
        "place_id": "ChIJQZDp3Jq8cEgRWj8OJd7cukU",
        "url": "https://www.google.com/maps/place/Canvas+The+Old+Fire+Station/data=!4m7!3m6!1s0x4870bc9adce99041:0x45badcde250e3f5a!8m2!3d52.4857565!4d-1.890775!16s%2Fg%2F11b6xdj_60!19sChIJQZDp3Jq8cEgRWj8OJd7cukU?hl=en",
        "query": "Canvas The Old Fire Station Birmingham",
        "address": "Aston Street, Birmingham B4 7DA"
    },
    {
        "house_id": 1481668,
        "title": "Canvas Coventry Arundel House",
        "place_id": "ChIJ44R9APxLd0gRhWJq2r_z528",
        "url": None,
        "query": "Canvas Coventry Arundel House Whitefriars Lane Coventry CV1 2NA",
        "address": "Whitefriars Lane, Coventry CV1 2NA"
    },
    {
        "house_id": 798336,
        "title": "Canvas Glasgow Boyce House",
        "place_id": "ChIJF8niHmJFiEgRd4dgCJSPRC0",
        "url": "https://www.google.com/maps/place/Canvas+Glasgow/@55.8679971,-4.2465639,17z/data=!3m1!4b1!4m6!3m5!1s0x488845621ee28917:0x2d448f9408608777!8m2!3d55.8679971!4d-4.2465639!16s%2Fg%2F11f4_56y3b?hl=en",
        "query": "Canvas Glasgow Kyle Street",
        "address": "47 Kyle Street, Glasgow G4 0JQ"
    },
    {
        "house_id": 600769511,
        "title": "Canvas Clubhouse",
        "place_id": "ChIJo_lehVKlbUgRtjYa-0s6ZVA",
        "url": None,
        "query": "Canvas Student The Clubhouse 27 Mary Arches Street Exeter EX4 3FP",
        "address": "The Clubhouse, 27 Mary Arches Street, Exeter EX4 3FP"
    },
    {
        "house_id": 17378,
        "title": "Canvas Manchester River Street Tower",
        "place_id": "ChIJy79s9Lexe0gRrLSsaSyTjyA",
        "url": None,
        "query": "Canvas Manchester River Street Tower 2 River Street Manchester M15 5GQ",
        "address": "River Street Tower, 2 River Street, Manchester M15 5GQ"
    },
    {
        "house_id": 714828,
        "title": "Canvas Capital House",
        "place_id": "ChIJqVXc47R2dEgRlgsYa4vmRP8",
        "url": None,
        "query": "Canvas Capital House 1 Houndwell Place Southampton SO14 1BS",
        "address": "1 Houndwell Place, Southampton SO14 1BS"
    },
    {
        "house_id": 1507876,
        "title": "Canvas Bristol The Malthouse",
        "place_id": "ChIJldQOzGqPcUgRfKOQnXb-YPI",
        "url": None,
        "query": "Canvas Bristol The Malthouse 29 Wilder Street Bristol BS2 8QA",
        "address": "29 Wilder Street, Bristol BS2 8QA"
    },
    {
        "house_id": 1409929,
        "title": "Canvas Coventry City Club",
        "place_id": "ChIJ6-AoUMVLd0gRht57eXp1w58",
        "url": None,
        "query": "Canvas Coventry City Club 1 Gas Street Coventry CV1 4GJ",
        "address": "1 Gas Street, Coventry CV1 4GJ"
    },
    {
        "house_id": 599786464,
        "title": "Canvas Cardiff The Parade",
        "place_id": None,
        "url": None,
        "query": "Canvas Student The Parade 35 The Parade Cardiff CF24 3AD",
        "address": "35 The Parade, Cardiff CF24 3AD"
    },
    {
        "house_id": 1466060,
        "title": "Canvas Coventry City Point",
        "place_id": "ChIJ8yIpkhpLd0gREZGt3xaQKKo",
        "url": None,
        "query": "Canvas Coventry City Point 2 Gas Street Coventry CV1 4FY",
        "address": "2 Gas Street, Coventry CV1 4FY"
    },
    {
        "house_id": 600310759,
        "title": "Canvas Wembley Arthur House",
        "place_id": None,
        "url": None,
        "query": "Canvas Student Arthur House 1 Rutherford Way Wembley HA9 0BP",
        "address": "Arthur House, 1 Rutherford Way, Wembley HA9 0BP"
    },
    {
        "house_id": 1475335,
        "title": "Canvas Manchester",
        "place_id": "ChIJ01b97g-xe0gRg-pQflvvAc8",
        "url": "https://www.google.com/maps/place/Canvas+Manchester/@53.4712812,-2.2491514,17z/data=!3m1!4b1!4m6!3m5!1s0x487bb1505877ea01:0xc801ef504b75d20e!8m2!3d53.4712812!4d-2.2491514!16s%2Fg%2F11lhfnz84r?hl=en",
        "query": "Canvas Manchester 2 River Street Manchester",
        "address": "River Street Tower, 2 River Street, Manchester M15 5GQ"
    },
    {
        "house_id": 1577212,
        "title": "Canvas The Heights",
        "place_id": None,
        "url": None,
        "query": "Canvas The Heights Staniforth Street Birmingham",
        "address": "30 Staniforth Street, Birmingham B4 7DR"
    },
    {
        "house_id": 475792259,
        "title": "Canvas Wembley Barnard Point",
        "place_id": "ChIJ1YpMCX0RdkgROJMuhp_jKvA",
        "url": None,
        "query": "Canvas Student Barnard Point 577 North End Road Wembley London HA9 0UU",
        "address": "577 North End Road, Wembley, London HA9 0UU"
    },
    {
        "house_id": 927794504,
        "title": "Quintana Gate",
        "place_id": "ChIJo_lehVKlbUgRtjYa-0s6ZVA",
        "url": "https://www.google.com/maps/place/Canvas+Student+-+Quintana+Gate/@50.7227297,-3.5356283,17z/data=!3m1!4b1!4m6!3m5!1s0x486da552855ef9a3:0x50653a4bfb1a36b6!8m2!3d50.7227297!4d-3.5356283!16s%2Fg%2F11fmgbqncx?hl=en",
        "query": "Canvas Student Quintana Gate Exeter",
        "address": "27 Mary Arches Street, Exeter EX4 3FP"
    },
    {
        "house_id": 1481666,
        "title": "Canvas Walthamstow",
        "place_id": "ChIJcer7_7oddkgR-bJ4Wo3AKSA",
        "url": None,
        "query": "Canvas Walthamstow 4 Forest Road London E17 6JJ",
        "address": "4 Forest Road, Walthamstow, London E17 6JJ"
    }
]

CSV_COLUMNS = [
    "id", "house_id", "user_id", "reviewer_name", "reviewer_email", "rating",
    "star_num", "comment", "school_name", "unit_name", "status", "source",
    "created_at", "updated_at", "deleted_at", "translated_comment"
]

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                cleaned = {k: v for k, v in data.items() if v and len(v) > 0}
                return cleaned
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")
    return {}

def save_cache(cache_dict):
    try:
        temp_file = CACHE_FILE + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(cache_dict, f, ensure_ascii=False, indent=2)
        os.replace(temp_file, CACHE_FILE)
    except Exception as e:
        logger.error(f"Error saving cache: {e}")

def parse_relative_date(date_str):
    now = datetime.now(timezone.utc)
    if not date_str:
        return now.strftime("%Y-%m-%d %H:%M:%S+00")
    s = date_str.lower()
    m = re.search(r"(\d+)\s*(day|week|month|year|hour|minute)s?\s*ago", s)
    if m:
        val = int(m.group(1))
        unit = m.group(2)
        if unit == "minute": delta = timedelta(minutes=val)
        elif unit == "hour": delta = timedelta(hours=val)
        elif unit == "day": delta = timedelta(days=val)
        elif unit == "week": delta = timedelta(weeks=val)
        elif unit == "month": delta = timedelta(days=val * 30)
        elif unit == "year": delta = timedelta(days=val * 365)
        else: delta = timedelta(0)
        return (now - delta).strftime("%Y-%m-%d %H:%M:%S+00")
    if "a month ago" in s or "a month" in s:
        return (now - timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S+00")
    if "a year ago" in s or "a year" in s:
        return (now - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S+00")
    if "a week ago" in s or "a week" in s:
        return (now - timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S+00")
    if "yesterday" in s or "a day ago" in s:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S+00")
    return now.strftime("%Y-%m-%d %H:%M:%S+00")

def load_env():
    env_vars = {}
    env_file = CURRENT_DIR / ".env"
    if env_file.exists():
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, v = line.split("=", 1)
                    env_vars[k.strip()] = v.strip().strip('"').strip("'")
    return env_vars

def scrape_single_property(page, prop_info, max_scrolls=6):
    """
    Scrapes reviews using selectors and multi-step strategy from google-maps-scraper.
    """
    direct_url = prop_info.get("url")
    place_id = prop_info.get("place_id")
    query = prop_info.get("query")
    
    if direct_url:
        target_url = direct_url
    elif place_id and place_id != "N/A" and not place_id.startswith("EhxFd"):
        target_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
    else:
        target_url = f"https://www.google.com/maps/search/{urllib.parse.quote(query)}?hl=en"
        
    page.goto(target_url, wait_until="networkidle", timeout=30000)
    time.sleep(2)
    
    # 1. Dismiss cookies/consent modal
    page.evaluate('''() => {
        for (const b of document.querySelectorAll('button')) {
            const t = (b.textContent || '').toLowerCase();
            if (t.includes('accept all') || t.includes('agree') || t.includes('dismiss') || t.includes('reject all')) {
                b.click();
                return;
            }
        }
    }''')
    time.sleep(1)
    
    # 2. If on search results page, click the first place listing
    page.evaluate('''() => {
        const first = document.querySelector('div.Nv2PK a.hfpxzc, div.Nv2PK a');
        if (first) first.click();
    }''')
    time.sleep(2)
    
    # 3. Open Reviews panel (methods from google-maps-scraper)
    page.evaluate('''() => {
        // Method 1: Reviews tab
        for (const tab of document.querySelectorAll('button[role="tab"]')) {
            const label = (tab.getAttribute('aria-label') || tab.textContent || '').toLowerCase();
            if (label.includes('review')) {
                tab.click();
                return;
            }
        }
        
        // Method 2: Review buttons / action buttons
        const reviewsButtons = document.querySelectorAll('button[jsaction*="reviewChart"], button[jsaction*="reviews"]');
        for (const btn of reviewsButtons) {
            if ((btn.textContent || '').toLowerCase().includes('review') || (btn.getAttribute('aria-label') || '').toLowerCase().includes('review')) {
                btn.click();
                return;
            }
        }

        // Method 3: Rating area (.F7nice)
        const ratingArea = document.querySelector('.F7nice');
        if (ratingArea) {
            ratingArea.click();
            return;
        }

        // Method 4: Links or buttons with 'more reviews' or 'all reviews'
        for (const b of document.querySelectorAll('button, a')) {
            const t = (b.textContent || '').toLowerCase();
            const a = (b.getAttribute('aria-label') || '').toLowerCase();
            if (t.includes('more review') || t.includes('all review') || a.includes('more review') || (t.includes('review') && !t.includes('write'))) {
                b.click();
                return;
            }
        }
    }''')
    time.sleep(2)
    
    # 4. Scroll feed and extract reviews
    collected = {}
    for scroll_idx in range(max_scrolls):
        # Click "See more" / "More" buttons on cards
        page.evaluate('''() => {
            document.querySelectorAll('button.w8nwRe, button[aria-label*="See more"], button[aria-label*="More"]').forEach(b => {
                try { b.click(); } catch(e) {}
            });
        }''')
        
        items = page.evaluate(r'''() => {
            const reviewSelectors = [
                '.jftiEf',
                'div[data-review-id]',
                '.gws-localreviews__google-review',
                '[data-hveid] .review-dialog-list > div',
                '.WMbnJf',
                '.bwb7ce'
            ];
            let reviewElements = [];
            for (const selector of reviewSelectors) {
                const elements = document.querySelectorAll(selector);
                if (elements && elements.length > 0) {
                    reviewElements = Array.from(elements);
                    break;
                }
            }
            
            return reviewElements.map(element => {
                // Author name
                const userSelectors = ['.d4r55', '.WNxzHc', '.TSUbDb a', '.review-author', 'button.al6Kxe', '.bHrnEe'];
                let userName = '';
                for (const sel of userSelectors) {
                    const el = element.querySelector(sel);
                    if (el && el.textContent?.trim()) {
                        userName = el.textContent.trim();
                        break;
                    }
                }
                
                // Rating
                let rating = null;
                const ratingSelectors = ['.kvMYJc', '.DU9Pgb span[aria-label]', '[role="img"][aria-label*="star"]', '.pjemBf span', '.review-score'];
                for (const sel of ratingSelectors) {
                    const ratingEl = element.querySelector(sel);
                    if (ratingEl) {
                        const ariaLabel = ratingEl.getAttribute('aria-label') || '';
                        const match = ariaLabel.match(/([0-9]+(\.[0-9]+)?)/);
                        if (match) {
                            rating = parseFloat(match[1]);
                            break;
                        }
                    }
                }
                
                // Date
                const timeSelectors = ['.rsqaWe', '.DU9Pgb', '.tTVLSc', '.review-date', '.dehysf'];
                let relativeTime = '';
                for (const sel of timeSelectors) {
                    const el = element.querySelector(sel);
                    if (el && el.textContent?.trim()) {
                        relativeTime = el.textContent.trim();
                        break;
                    }
                }
                
                // Text
                const textSelectors = ['.wiI7pd', '.MyEned span', '.review-full-text', '.Jtu6Td span', '[data-expandable-section] span'];
                let text = '';
                for (const sel of textSelectors) {
                    const el = element.querySelector(sel);
                    if (el && el.textContent?.trim()) {
                        text = el.textContent.trim();
                        break;
                    }
                }
                
                return {
                    author: userName || 'Anonymous',
                    rating: rating !== null ? rating : 5.0,
                    text: text,
                    date: relativeTime
                };
            });
        }''')
        
        for it in items:
            key = (it["author"], it["text"][:30])
            if key not in collected and it["author"] != 'Anonymous':
                collected[key] = it
                
        # Scroll scrollable container (selectors from google-maps-scraper)
        page.evaluate('''() => {
            const scrollSelectors = [
                '.m6QErb.DxyBCb.kA9KIf.dS8AEf',
                '.m6QErb.DxyBCb.kA9KIf',
                '.DxyBCb.kA9KIf',
                '.m6QErb.DxyBCb',
                '.m6QErb',
                '.section-scrollbox',
                'div[role="feed"]',
                'div[role="main"]'
            ];
            for (const selector of scrollSelectors) {
                const el = document.querySelector(selector);
                if (el && el.scrollHeight > el.clientHeight) {
                    el.scrollTop = el.scrollHeight;
                    return true;
                }
            }
            return false;
        }''')
        time.sleep(1.2)
        
        if len(collected) >= 5:
            break
            
    return list(collected.values())

def export_reviews(cache):
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    all_reviews = []
    
    for prop in TARGET_PROPERTIES:
        hid = prop["house_id"]
        rev_list = cache.get(str(hid), [])
        for r in rev_list:
            all_reviews.append({
                "house_id": hid,
                "author": r.get("author") or "Anonymous",
                "rating": r.get("rating") if r.get("rating") is not None else 5.0,
                "comment": r.get("text") or "",
                "created_at": parse_relative_date(r.get("date"))
            })
            
    # Write CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for idx, r in enumerate(all_reviews, start=1):
            star = int(round(r["rating"]))
            writer.writerow({
                "id": idx,
                "house_id": r["house_id"],
                "user_id": "",
                "reviewer_name": r["author"],
                "reviewer_email": "",
                "rating": f"{r['rating']:.2f}",
                "star_num": star,
                "comment": r["comment"],
                "school_name": "",
                "unit_name": "",
                "status": "approved",
                "source": "google_review",
                "created_at": r["created_at"],
                "updated_at": now_ts,
                "deleted_at": "",
                "translated_comment": ""
            })
            
    # Write SQL
    with open(OUTPUT_SQL, "w", encoding="utf-8") as f:
        f.write("-- Google Maps Reviews for 16 Requested Properties\n")
        f.write("-- Table: new_house_reviews\n\n")
        for idx, r in enumerate(all_reviews, start=1):
            hid = r["house_id"]
            author = (r["author"]).replace("'", "''")
            rating_val = f"{r['rating']:.2f}"
            star_val = str(int(round(r["rating"])))
            comment = (r["comment"]).replace("'", "''")
            created_at = r["created_at"]
            f.write(
                f"INSERT INTO new_house_reviews ("
                f"house_id, user_id, reviewer_name, reviewer_email, rating, star_num, comment, "
                f"school_name, unit_name, status, source, created_at, updated_at) "
                f"VALUES ("
                f"{hid}, NULL, '{author}', NULL, {rating_val}, {star_val}, '{comment}', "
                f"NULL, NULL, 'approved', 'google_review', '{created_at}', '{now_ts}');\n"
            )
            
    return len(all_reviews)

def seed_databases():
    if not os.path.exists(OUTPUT_CSV):
        logger.error(f"Cannot seed: {OUTPUT_CSV} does not exist.")
        return
        
    with open(OUTPUT_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        reviews = list(reader)
        
    logger.info(f"Loaded {len(reviews)} reviews from {OUTPUT_CSV} for database seeding.")
    prop_dict = {p["house_id"]: p for p in TARGET_PROPERTIES}

    # 1. Seed SQLite
    if os.path.exists(SQLITE_PATH):
        try:
            import sqlite3
            conn = sqlite3.connect(SQLITE_PATH)
            cur = conn.cursor()
            
            cur.execute("SELECT MAX(CAST(id AS INTEGER)) FROM new_house_reviews;")
            max_id_row = cur.fetchone()
            next_id = (max_id_row[0] or 0) + 1 if max_id_row and max_id_row[0] is not None else 1
            
            inserted_sqlite = 0
            for r in reviews:
                cur.execute(
                    "SELECT id FROM new_house_reviews WHERE house_id=? AND reviewer_name=? AND comment=?",
                    (str(r["house_id"]), r["reviewer_name"], r["comment"])
                )
                if not cur.fetchone():
                    rating_val = f"{float(r['rating']):.2f}" if r.get("rating") else "5.00"
                    star_val = str(int(round(float(r["star_num"])))) if r.get("star_num") else "5"
                    cur.execute("""
                        INSERT INTO new_house_reviews (
                            id, house_id, user_id, reviewer_name, reviewer_email, rating,
                            star_num, comment, school_name, unit_name, status, source,
                            created_at, updated_at
                        ) VALUES (?, ?, NULL, ?, NULL, ?, ?, ?, NULL, NULL, 'approved', 'google_review', ?, ?)
                    """, (
                        str(next_id),
                        str(r["house_id"]),
                        r["reviewer_name"],
                        rating_val,
                        star_val,
                        r["comment"],
                        r["created_at"],
                        r["updated_at"]
                    ))
                    next_id += 1
                    inserted_sqlite += 1
                    
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
            for p in TARGET_PROPERTIES:
                hid = p["house_id"]
                cur.execute("SELECT COUNT(*), AVG(CAST(rating AS FLOAT)) FROM new_house_reviews WHERE house_id = ?;", (str(hid),))
                res = cur.fetchone()
                cnt = res[0] if res and res[0] else 0
                avg_r = f"{round(float(res[1]), 2):.2f}" if res and res[1] else "5.00"
                
                cur.execute("SELECT house_id FROM house_business_mappings WHERE house_id = ?;", (str(hid),))
                if cur.fetchone():
                    cur.execute("""
                        UPDATE house_business_mappings 
                        SET review_count = ?, rating = ?, updated_at = ? 
                        WHERE house_id = ?;
                    """, (str(cnt), avg_r, now_iso, str(hid)))
                else:
                    cur.execute("""
                        INSERT INTO house_business_mappings (
                            house_id, business_id, place_id, business_name, business_address, review_count, rating, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """, (
                        str(hid),
                        p.get("place_id") or str(hid),
                        p.get("place_id") or "",
                        p["title"],
                        p["address"],
                        str(cnt),
                        avg_r,
                        now_iso,
                        now_iso
                    ))
            conn.commit()
            conn.close()
            logger.info(f"SQLite: inserted {inserted_sqlite} new reviews and updated house_business_mappings.")
        except Exception as e:
            logger.error(f"Error seeding SQLite: {e}")

    # 2. Seed PostgreSQL
    env_vars = load_env()
    try:
        import psycopg2
        from psycopg2.extras import execute_batch
        
        conn = psycopg2.connect(
            host=env_vars.get("DB_HOST", "165.99.228.81"),
            port=env_vars.get("DB_PORT", "6432"),
            dbname=env_vars.get("DB_NAME", "acolye-living-v2"),
            user=env_vars.get("DB_USER", "al_all"),
            password=env_vars.get("DB_PASSWORD"),
            connect_timeout=10
        )
        cur = conn.cursor()
        
        hids = [p["house_id"] for p in TARGET_PROPERTIES]
        cur.execute("SELECT house_id, reviewer_name, comment FROM new_house_reviews WHERE house_id = ANY(%s);", (hids,))
        existing_set = set(cur.fetchall())
        
        to_insert = []
        for r in reviews:
            key = (int(r["house_id"]), r["reviewer_name"], r["comment"])
            if key not in existing_set:
                to_insert.append((
                    int(r["house_id"]),
                    r["reviewer_name"],
                    float(r["rating"]) if r["rating"] else 5.0,
                    int(r["star_num"]) if r["star_num"] else 5,
                    r["comment"],
                    "approved",
                    "google_review",
                    r["created_at"],
                    r["updated_at"]
                ))
                existing_set.add(key)
                
        if to_insert:
            query = """
                INSERT INTO new_house_reviews (
                    house_id, reviewer_name, rating, star_num, comment, status, source, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """
            execute_batch(cur, query, to_insert, page_size=100)
            conn.commit()
            logger.info(f"PostgreSQL: inserted {len(to_insert)} new reviews into new_house_reviews.")
        else:
            logger.info("PostgreSQL: all reviews already exist, none inserted.")
            
        # Upsert house_business_mappings
        for p in TARGET_PROPERTIES:
            hid = p["house_id"]
            cur.execute("SELECT COUNT(*), AVG(rating) FROM new_house_reviews WHERE house_id = %s;", (hid,))
            cnt, avg_r = cur.fetchone()
            cnt_val = cnt if cnt else 0
            avg_val = round(float(avg_r), 2) if avg_r else 5.0
            
            cur.execute("""
                INSERT INTO house_business_mappings (
                    house_id, business_id, place_id, business_name, business_address, match_status, review_count, rating, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, 'Resolved', %s, %s, NOW(), NOW())
                ON CONFLICT (house_id) DO UPDATE
                SET review_count = EXCLUDED.review_count,
                    rating = EXCLUDED.rating,
                    updated_at = NOW();
            """, (
                hid,
                p.get("place_id") or str(hid),
                p.get("place_id") or "",
                p["title"],
                p["address"],
                cnt_val,
                avg_val
            ))
        conn.commit()
        conn.close()
        logger.info(f"PostgreSQL: successfully updated/upserted mappings for all {len(TARGET_PROPERTIES)} properties.")
    except Exception as e:
        logger.error(f"Error seeding PostgreSQL: {e}")

def main():
    logger.info(f"Starting review scrape for {len(TARGET_PROPERTIES)} properties...")
    cache = load_cache()
    logger.info(f"Currently cached: {len(cache)} properties with reviews.")
    
    pending = [p for p in TARGET_PROPERTIES if str(p["house_id"]) not in cache]
    logger.info(f"Remaining properties to scrape: {len(pending)}")
    
    if pending:
        with sync_playwright() as p:
            browser = p.firefox.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                locale="en-US"
            )
            page = context.new_page()
            
            # Dismiss cookie banner once at start
            page.goto("https://www.google.com/maps?hl=en", wait_until="networkidle", timeout=30000)
            time.sleep(2)
            page.evaluate('''() => {
                for (const b of document.querySelectorAll('button')) {
                    const t = (b.textContent || '').toLowerCase();
                    if (t.includes('accept all') || t.includes('agree') || t.includes('dismiss') || t.includes('reject all')) {
                        b.click();
                        return;
                    }
                }
            }''')
            time.sleep(1)
            
            try:
                for idx, prop in enumerate(pending, start=1):
                    hid = prop["house_id"]
                    title = prop["title"]
                    logger.info(f"[{idx}/{len(pending)}] Scraping House ID {hid}: '{title}'...")
                    try:
                        revs = scrape_single_property(page, prop, max_scrolls=6)
                        logger.info(f"  -> Extracted {len(revs)} reviews for House ID {hid}.")
                        if revs:
                            cache[str(hid)] = revs
                            save_cache(cache)
                    except Exception as e:
                        logger.error(f"  ✗ Error scraping {hid} ({title}): {e}")
                        
                    export_reviews(cache)
                    time.sleep(1)
            finally:
                page.close()
                context.close()
                browser.close()
                
    total_revs = export_reviews(cache)
    logger.info(f"Scraping completed! Exported {total_revs} reviews across {len(cache)} properties.")
    seed_databases()

if __name__ == "__main__":
    main()
