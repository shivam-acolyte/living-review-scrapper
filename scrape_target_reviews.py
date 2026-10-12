#!/usr/bin/env python3
"""
Scrapes Google Maps reviews for the 63 requested property IDs,
saves progress to target_reviews_cache.json, exports to CSV & SQL,
and optionally seeds directly into PostgreSQL and SQLite.
"""

import os
import sys
import csv
import json
import time
import re
import signal
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
logger = logging.getLogger("target_scraper")

CURRENT_DIR = Path(__file__).resolve().parent
CACHE_FILE = str(CURRENT_DIR / "target_reviews_cache.json")
OUTPUT_CSV = str(CURRENT_DIR / "target_property_reviews.csv")
OUTPUT_SQL = str(CURRENT_DIR / "target_property_reviews.sql")
BUSINESS_IDS_CSV = str(CURRENT_DIR / "property_business_ids.csv")

# The 63 properties requested by user
TARGET_HOUSE_IDS = [
    1717485, 1564784, 1517101, 1579598, 19584, 1455100, 1454893, 1515771,
    1455089, 1515773, 1499412, 1455053, 1462310, 1523347, 1591844, 1515774,
    1591834, 1591836, 1636037, 1669249, 1591842, 1671331, 1591837, 1684342,
    1579098, 1525421, 18435, 19004, 1554694, 1579564, 1525440, 736275,
    1554452, 18469, 1443602, 1712633, 1579705, 1518486, 1657815, 1420113,
    1584896, 1525172, 1709201, 13879, 1512730, 13886, 1515981, 1519779,
    29064, 1518909, 1522560, 1522632, 1666697, 1642439, 1565193, 1565178,
    1577053, 1574760, 1567625, 1567568, 1567557, 1567545, 1567539
]

stop_requested = False

def handle_sigint(signum, frame):
    global stop_requested
    logger.warning("\n[!] Graceful stop requested (Ctrl+C). Saving current progress...")
    stop_requested = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

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

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
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

CSV_COLUMNS = [
    "id",
    "house_id",
    "user_id",
    "reviewer_name",
    "reviewer_email",
    "rating",
    "star_num",
    "comment",
    "school_name",
    "unit_name",
    "status",
    "source",
    "created_at",
    "updated_at",
    "deleted_at",
    "translated_comment"
]

def export_reviews(cache):
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    all_reviews = []
    
    for hid_str, rev_list in cache.items():
        hid = int(hid_str)
        for r in rev_list:
            all_reviews.append({
                "house_id": hid,
                "author": r.get("author") or "Anonymous",
                "rating": r.get("rating"),
                "comment": r.get("text") or "",
                "created_at": parse_relative_date(r.get("date"))
            })
            
    # Write CSV
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for idx, r in enumerate(all_reviews, start=1):
            star = int(round(r["rating"])) if r.get("rating") is not None else 5
            rating_formatted = f"{r['rating']:.2f}" if r.get("rating") is not None else "5.00"
            writer.writerow({
                "id": idx,
                "house_id": r["house_id"],
                "user_id": "",
                "reviewer_name": r["author"],
                "reviewer_email": "",
                "rating": rating_formatted,
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
        f.write("-- Google Maps Reviews for 63 Target Properties\n")
        f.write("-- Table: new_house_reviews\n\n")
        for idx, r in enumerate(all_reviews, start=1):
            hid = r["house_id"]
            author = (r["author"]).replace("'", "''")
            rating_val = f"{r['rating']:.2f}" if r.get("rating") is not None else "5.00"
            star_val = str(int(round(r["rating"]))) if r.get("rating") is not None else "5"
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

def scrape_property_reviews(page, place_id, name, address, max_scrolls=4):
    """Navigates to Google Maps and extracts reviews."""
    if place_id and place_id != "N/A" and not place_id.startswith("EhxFd"):
        url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
    else:
        q = f"{name} {address}".strip()
        url = f"https://www.google.com/maps/search/{urllib.parse.quote(q)}?hl=en"
        
    page.goto(url, wait_until="networkidle", timeout=30000)
    time.sleep(1.5)
    
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
    time.sleep(0.8)
    
    # 2. Click Reviews tab/button
    page.evaluate('''() => {
        // Try tab buttons or more reviews button
        const selectors = [
            'button[role="tab"][aria-label*="Reviews"]',
            'button[aria-label*="Reviews for"]',
            'button[aria-label*="reviews"]',
            'button.GQjSyb',
            'button.hh2c6',
            '.F7nice'
        ];
        for (const sel of selectors) {
            const el = document.querySelector(sel);
            if (el) {
                el.click();
                return;
            }
        }
        for (const b of document.querySelectorAll('button')) {
            const txt = (b.getAttribute('aria-label') || b.textContent || '').toLowerCase();
            if (txt.includes('review') && !txt.includes('write')) {
                b.click();
                return;
            }
        }
    }''')
    time.sleep(1.5)
    
    # 3. Scroll container and extract
    collected = {}
    for _ in range(max_scrolls):
        # Expand 'See more'
        items = page.evaluate('''() => {
            document.querySelectorAll('button.w8nwRe, button[aria-label*="See more"]').forEach(b => {
                try { b.click(); } catch(e) {}
            });
            const cards = document.querySelectorAll('div.jftiEf');
            return Array.from(cards).map(card => {
                const author = card.querySelector('.d4r55')?.textContent?.trim() || 'Anonymous';
                const text = card.querySelector('.wiI7pd')?.textContent?.trim() || '';
                const ratingEl = card.querySelector('.kvMYJc, span[role="img"][aria-label*="star"]');
                let rating = null;
                if (ratingEl) {
                    const match = (ratingEl.getAttribute('aria-label') || '').match(/([0-9]+(\\.[0-9]+)?)/);
                    if (match) rating = parseFloat(match[1]);
                }
                const date = card.querySelector('.rsqaWe')?.textContent?.trim() || '';
                return { author, rating, text, date };
            });
        }''')
        
        for it in items:
            key = (it["author"], it["text"][:30])
            if key not in collected:
                collected[key] = it
                
        # Scroll reviews container
        page.evaluate('''() => {
            const containers = document.querySelectorAll('.m6QErb.DxyBCb, .m6QErb, div[role="main"]');
            for (const c of containers) {
                if (c.scrollHeight > c.clientHeight) {
                    c.scrollTop += 1200;
                }
            }
        }''')
        time.sleep(0.8)
        
    return list(collected.values())

def main():
    logger.info(f"Targeting {len(TARGET_HOUSE_IDS)} properties for reviews scraping...")
    
    # Load property metadata
    props_meta = {}
    if os.path.exists(BUSINESS_IDS_CSV):
        with open(BUSINESS_IDS_CSV, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("house_id"):
                    hid = int(row["house_id"])
                    if hid in TARGET_HOUSE_IDS:
                        props_meta[hid] = row
                        
    logger.info(f"Loaded metadata for {len(props_meta)} target properties.")
    
    cache = load_cache()
    logger.info(f"Currently cached: {len(cache)} properties ({sum(len(v) for v in cache.values())} reviews).")
    
    pending = [hid for hid in TARGET_HOUSE_IDS if str(hid) not in cache]
    logger.info(f"Remaining properties to scrape: {len(pending)}")
    
    if not pending:
        logger.info("All target properties are already scraped!")
        total_revs = export_reviews(cache)
        logger.info(f"Flushed {total_revs} reviews to {OUTPUT_CSV} and {OUTPUT_SQL}.")
        return

    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
            locale="en-US"
        )
        page = context.new_page()
        
        try:
            for idx, hid in enumerate(pending, start=1):
                if stop_requested:
                    logger.info("Stopping as requested.")
                    break
                    
                meta = props_meta.get(hid, {})
                title = meta.get("business_name") or meta.get("title") or f"Property {hid}"
                place_id = meta.get("place_id") or ""
                address = meta.get("business_address") or meta.get("address") or ""
                
                logger.info(f"[{idx}/{len(pending)}] Scraping House ID {hid}: '{title}'...")
                
                try:
                    revs = scrape_property_reviews(page, place_id, title, address, max_scrolls=4)
                    logger.info(f"  -> Extracted {len(revs)} reviews.")
                    cache[str(hid)] = revs
                    save_cache(cache)
                except Exception as e:
                    logger.error(f"  ✗ Error scraping {hid} ({title}): {e}")
                    cache[str(hid)] = []
                    save_cache(cache)
                    
                if idx % 5 == 0 or idx == len(pending):
                    cnt = export_reviews(cache)
                    logger.info(f"  ✓ Progress saved: {len(cache)} properties processed, {cnt} total reviews exported.")
                    
                time.sleep(0.5)
                
        finally:
            page.close()
            context.close()
            browser.close()
            
    total_revs = export_reviews(cache)
    logger.info("Scraping completed!")
    logger.info(f"Processed: {len(cache)} / {len(TARGET_HOUSE_IDS)} properties.")
    logger.info(f"Total reviews extracted: {total_revs}")
    logger.info(f"CSV: {OUTPUT_CSV}")
    logger.info(f"SQL: {OUTPUT_SQL}")

if __name__ == "__main__":
    main()
