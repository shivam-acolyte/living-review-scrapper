#!/usr/bin/env python3
"""
image_review.py — Google Maps Review Image Scraper + Downloader

Reads house_ids from  target.txt  (one per line, # = comment),
looks up the corresponding place_id in property_business_ids.csv,
navigates to each property's Google Maps listing, and:

  1. Extracts reviewer-attached photo URLs (NOT profile pictures)
  2. Downloads each image to  review_images_dl/<house_id>/<sha256[:16]>.jpg
  3. Computes SHA-256 hash of the downloaded file
  4. Appends a row to  review_images.csv

Output files:
  review_images.csv          — metadata (house_id, review_key, url, hash, local_path, …)
  review_images_dl/<hid>/    — downloaded image files

Cache files:
  image_done.json            — house_ids fully processed (skip on re-run)
  image_no_reviews.json      — house_ids with zero review photos (skip permanently)

Usage:
  python image_review.py
"""

import os, sys, csv, json, time, re, signal, logging, hashlib
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import URLError

from playwright.sync_api import sync_playwright

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("image_review")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR        = Path(__file__).resolve().parent
TARGET_FILE     = BASE_DIR / "targetimg.txt"
INPUT_CSV       = BASE_DIR / "property_business_ids.csv"
OUTPUT_CSV      = BASE_DIR / "review_images.csv"
DOWNLOAD_DIR    = BASE_DIR / "review_images_dl"
DONE_CACHE      = BASE_DIR / "image_done.json"
NO_IMAGES_CACHE = BASE_DIR / "image_no_reviews.json"

# ---------------------------------------------------------------------------
# Tuning
# ---------------------------------------------------------------------------
SCROLL_SLEEP    = 1.2   # seconds between scroll steps
NAV_SLEEP       = 2.0   # seconds after page navigation
BETWEEN_SLEEP   = 1.0   # seconds between properties
STALL_ROUNDS    = 5     # stop scrolling after N rounds with no new cards
DOWNLOAD_TIMEOUT = 15   # seconds per image download
DOWNLOAD_RETRIES = 3    # retry count on failure

# HTTP headers to appear as a real browser when downloading
DOWNLOAD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
        "Gecko/20100101 Firefox/128.0"
    ),
    "Referer": "https://www.google.com/maps/",
    "Accept": "image/avif,image/webp,image/png,image/jpeg,*/*",
}

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

# ---------------------------------------------------------------------------
# URL utilities
# ---------------------------------------------------------------------------
def is_review_photo_url(url: str) -> bool:
    """
    Returns True only for reviewer-attached photo URLs.
    Filters OUT profile/avatar picture URLs.

    Google profile pictures have paths like:
      /a/ACg8ocK...   (updated avatar)
      /a-/ALV-UjX...  (old avatar)
    
    Actual review photos have paths like:
      /p/AF1Qip...    (place/review photo)
      /local-reviews/AJMZ0Qe...
    """
    if not url or not url.startswith("http"):
        return False
    # Must be Google's CDN
    if "googleusercontent.com" not in url and "ggpht.com" not in url:
        return False
    # Exclude profile avatar paths
    if re.search(r"/a-?/[A-Z]", url):
        return False
    # Accept confirmed review photo paths
    if "/p/AF1" in url or "/local-reviews/" in url:
        return True
    # Accept other non-avatar paths (e.g. /p/ variants)
    if re.search(r"/p/[A-Za-z0-9]", url):
        return True
    return False

def upgrade_url_resolution(url: str) -> str:
    """Strip all size/format suffixes and request a high-res version."""
    # Remove everything after the last '=' parameter block
    url = re.sub(r"=[\w\-+]+$", "=w1200-h1200-k-no", url)
    return url

# ---------------------------------------------------------------------------
# Image download + hash
# ---------------------------------------------------------------------------
def download_image(url: str, dest_path: Path) -> str | None:
    """
    Downloads image from url to dest_path.
    Returns SHA-256 hex digest of the file content, or None on failure.
    """
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            req = Request(url, headers=DOWNLOAD_HEADERS)
            with urlopen(req, timeout=DOWNLOAD_TIMEOUT) as resp:
                data = resp.read()
            if not data:
                logger.warning("    Empty response for %s", url)
                return None
            sha256 = hashlib.sha256(data).hexdigest()
            dest_path.write_bytes(data)
            return sha256
        except URLError as e:
            if attempt < DOWNLOAD_RETRIES:
                logger.debug("    Download attempt %d failed (%s) — retrying…", attempt, e)
                time.sleep(1.5 * attempt)
            else:
                logger.warning("    Download failed after %d attempts: %s — %s", DOWNLOAD_RETRIES, url, e)
                return None
        except Exception as e:
            logger.warning("    Unexpected download error: %s", e)
            return None

def compute_hash_from_file(path: Path) -> str:
    """SHA-256 of an already-downloaded file."""
    return hashlib.sha256(path.read_bytes()).hexdigest()

# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    "house_id",
    "review_key",
    "image_url",
    "image_hash",       # SHA-256 of the downloaded file
    "local_path",       # relative path from BASE_DIR
    "download_status",  # 'ok' | 'failed' | 'skipped'
    "scraped_at",
]

def ensure_csv():
    if not OUTPUT_CSV.exists():
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=CSV_COLUMNS).writeheader()
        logger.info("Created: %s", OUTPUT_CSV)

def append_image_row(
    house_id: int,
    review_key: str,
    image_url: str,
    image_hash: str | None,
    local_path: str | None,
    status: str,
):
    ensure_csv()
    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        w.writerow({
            "house_id":       house_id,
            "review_key":     review_key,
            "image_url":      image_url,
            "image_hash":     image_hash or "",
            "local_path":     local_path or "",
            "download_status": status,
            "scraped_at":     now_ts,
        })

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------
def _load_json_dict(path: Path) -> dict:
    if path.exists():
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning("Cache load error (%s): %s", path.name, e)
    return {}

def _load_json_set(path: Path) -> set:
    d = _load_json_dict(path)
    if "done" in d and isinstance(d["done"], list):
        return set(str(x) for x in d["done"])
    return set(str(k) for k in d.keys())

def _save_json(path: Path, data):
    tmp = str(path) + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, str(path))
    except Exception as e:
        logger.error("Cache save error (%s): %s", path.name, e)

def load_done() -> set:
    return _load_json_set(DONE_CACHE)

def save_done(done: set):
    _save_json(DONE_CACHE, {"done": sorted(done)})

def load_no_images() -> dict:
    return _load_json_dict(NO_IMAGES_CACHE)

def save_no_images(no_img: dict):
    _save_json(NO_IMAGES_CACHE, no_img)

def record_no_images(no_img: dict, prop: dict):
    hid_str = str(prop["house_id"])
    no_img[hid_str] = {
        "house_id":    prop["house_id"],
        "title":       prop.get("title", ""),
        "place_id":    prop.get("place_id", ""),
        "recorded_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00"),
    }
    save_no_images(no_img)
    logger.info(
        "  ∅ No review photos found — house_id=%s ('%s') added to image_no_reviews.json",
        hid_str, prop.get("title", "?")
    )

# ---------------------------------------------------------------------------
# Input readers
# ---------------------------------------------------------------------------
def read_target_ids() -> list[int]:
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
# Core scraper
# ---------------------------------------------------------------------------
def scrape_review_images(page, place_id: str, title: str) -> dict:
    """
    Returns: { review_key: [high_res_url, ...] }
    Only includes reviewer-attached photos (not profile pictures).
    Scrolls until STALL_ROUNDS consecutive rounds yield no new cards.
    """
    url = f"https://www.google.com/maps/place/?q=place_id:{place_id}&hl=en"
    logger.info("    → %s", url)
    try:
        page.goto(url, wait_until="networkidle", timeout=40000)
    except Exception:
        page.goto(url, wait_until="domcontentloaded", timeout=40000)
    time.sleep(NAV_SLEEP)

    # Click "Reviews" tab
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

    # Dismiss banners
    page.evaluate('''() => {
        for (const b of document.querySelectorAll("button")) {
            const t = (b.textContent || "").toLowerCase();
            if (t.includes("accept all") || t.includes("agree") || t.includes("dismiss"))
                { b.click(); return; }
        }
    }''')
    time.sleep(0.5)

    collected   = {}
    stall_count = 0
    scroll_round = 0

    logger.info("    → Collecting all review cards (stall=%d)…", STALL_ROUNDS)

    while stall_count < STALL_ROUNDS:
        if stop_requested:
            break

        scroll_round += 1

        # Expand truncated review text
        page.evaluate('''() => {
            document.querySelectorAll("button.w8nwRe, button[aria-label*='See more']")
                .forEach(b => { try { b.click(); } catch(e) {} });
        }''')
        time.sleep(0.3)

        items = page.evaluate(r'''() => {
            return Array.from(document.querySelectorAll("div.jftiEf")).map(card => {
                const author = card.querySelector(".d4r55")?.textContent?.trim() || "Anonymous";
                const text   = card.querySelector(".wiI7pd")?.textContent?.trim() || "";

                // ── Reviewer-attached photos only ──────────────────────────
                // These appear inside specific photo-button containers.
                // We intentionally exclude the reviewer's avatar (profile pic).
                const photoContainers = card.querySelectorAll(
                    "button.Tya61d, " +          // primary photo button class
                    "div.KtCyie, " +              // alternate container
                    "div.lu68Rd, " +              // another photo holder
                    "button[data-photo-index], " + // indexed photo buttons
                    "button[aria-label*='Photo by']"
                );
                const images = [];
                photoContainers.forEach(container => {
                    container.querySelectorAll("img").forEach(img => {
                        let src = img.getAttribute("src") || img.getAttribute("data-src") || "";
                        if (!src || src.startsWith("data:")) return;
                        if (!images.includes(src)) images.push(src);
                    });
                });

                return { author, text, images };
            });
        }''')

        before = len(collected)
        for it in items:
            key = f"{it['author']}|{it['text'][:40]}"
            if key not in collected:
                collected[key] = {
                    "author": it["author"],
                    "text":   it["text"],
                    "images": list(it["images"]),
                }
            else:
                # Merge any newly loaded images for an already-seen review
                seen = set(collected[key]["images"])
                for img in it["images"]:
                    if img not in seen:
                        collected[key]["images"].append(img)
                        seen.add(img)

        new_this_round = len(collected) - before
        if new_this_round == 0:
            stall_count += 1
        else:
            stall_count = 0
            logger.info("    round %d — +%d cards, total: %d", scroll_round, new_this_round, len(collected))

        # Scroll the reviews panel down
        page.evaluate('''() => {
            [".m6QErb.DxyBCb", ".m6QErb", "div[role='main']", ".section-scrollbox"]
                .forEach(sel => {
                    document.querySelectorAll(sel).forEach(el => {
                        if (el.scrollHeight > el.clientHeight) el.scrollTop += 1200;
                    });
                });
        }''')
        time.sleep(SCROLL_SLEEP)

    # Filter to only reviews that have photos, upgrade URL resolution
    result = {}
    for key, data in collected.items():
        valid_urls = []
        for raw_url in data["images"]:
            if is_review_photo_url(raw_url):
                valid_urls.append(upgrade_url_resolution(raw_url))
        if valid_urls:
            result[key] = valid_urls

    total_imgs = sum(len(v) for v in result.values())
    logger.info(
        "    → %d rounds | %d reviews | %d with photos | %d photo URLs",
        scroll_round, len(collected), len(result), total_imgs,
    )
    return result

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 64)
    logger.info("image_review.py — Review Photo Scraper + Downloader")
    logger.info("=" * 64)

    target_ids = read_target_ids()
    if not target_ids:
        logger.error("target.txt is empty — add house_ids and re-run.")
        sys.exit(0)
    logger.info("Target house_ids (%d): %s", len(target_ids), target_ids)

    prop_index = load_property_index()
    logger.info("Property index loaded: %d entries", len(prop_index))

    resolved = []
    for hid in target_ids:
        row = prop_index.get(hid)
        if not row:
            logger.warning("house_id %s not found in CSV — skipping.", hid)
            continue
        pid = (row.get("place_id") or "").strip()
        if not pid or pid in ("N/A", ""):
            logger.warning("house_id %s has no place_id — skipping.", hid)
            continue
        resolved.append({
            "house_id": hid,
            "title":    row.get("title", "?"),
            "place_id": pid,
        })
    logger.info("Resolved %d properties with valid place_id.", len(resolved))

    if not resolved:
        logger.error("No valid properties.")
        sys.exit(0)

    done_ids  = load_done()
    no_images = load_no_images()

    skipped = [p for p in resolved if str(p["house_id"]) in no_images]
    if skipped:
        logger.info("Skipping %d already-confirmed no-photo properties:", len(skipped))
        for p in skipped:
            rec = no_images[str(p["house_id"])]
            logger.info("  ∅ house_id=%-10s  %s  (recorded: %s)",
                        p["house_id"], p["title"], rec.get("recorded_at","?"))

    pending = [
        p for p in resolved
        if str(p["house_id"]) not in done_ids
        and str(p["house_id"]) not in no_images
    ]
    logger.info(
        "Already done: %d  |  No-photos (skipped): %d  |  Pending: %d",
        len(done_ids), len(no_images), len(pending)
    )

    if not pending:
        logger.info("Nothing to process.")
        return

    ensure_csv()
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

    total_urls      = 0
    total_ok        = 0
    total_failed    = 0

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

        # Dismiss cookie banner once
        try:
            page.goto("https://www.google.com/maps?hl=en",
                      wait_until="networkidle", timeout=30000)
            time.sleep(1.5)
            page.evaluate('''() => {
                for (const b of document.querySelectorAll("button")) {
                    const t = (b.textContent || "").toLowerCase();
                    if (t.includes("accept all") || t.includes("agree") || t.includes("reject all"))
                        { b.click(); return; }
                }
            }''')
            time.sleep(1)
        except Exception as e:
            logger.warning("Cookie banner (non-fatal): %s", e)

        try:
            for idx, prop in enumerate(pending, start=1):
                if stop_requested:
                    logger.info("Stop requested.")
                    break

                hid   = prop["house_id"]
                title = prop["title"]
                pid   = prop["place_id"]

                logger.info("[%d/%d] house_id=%s  '%s'", idx, len(pending), hid, title)

                # --- Scrape image URLs ---
                try:
                    review_photos = scrape_review_images(page, pid, title)
                except Exception as e:
                    logger.error("  ✗ Scrape error: %s", e)
                    done_ids.add(str(hid))
                    save_done(done_ids)
                    continue

                if not review_photos:
                    record_no_images(no_images, prop)
                    done_ids.add(str(hid))
                    save_done(done_ids)
                    continue

                # Create per-house download folder
                house_dl_dir = DOWNLOAD_DIR / str(hid)
                house_dl_dir.mkdir(parents=True, exist_ok=True)

                prop_ok = prop_fail = 0

                for rev_key, urls in review_photos.items():
                    for raw_url in urls:
                        total_urls += 1

                        # Filename: first 16 hex chars of URL hash + .jpg
                        url_hash_prefix = hashlib.sha256(raw_url.encode()).hexdigest()[:16]
                        filename        = f"{url_hash_prefix}.jpg"
                        dest            = house_dl_dir / filename
                        rel_path        = str(dest.relative_to(BASE_DIR))

                        # Skip already-downloaded file (resume support)
                        if dest.exists() and dest.stat().st_size > 0:
                            sha256 = compute_hash_from_file(dest)
                            logger.debug("    ↩ Already exists: %s (hash: %s…)", filename, sha256[:12])
                            append_image_row(hid, rev_key, raw_url, sha256, rel_path, "ok")
                            prop_ok += 1
                            total_ok += 1
                            continue

                        # Download
                        logger.info("    ↓ %s → %s", raw_url[:70], filename)
                        sha256 = download_image(raw_url, dest)

                        if sha256:
                            append_image_row(hid, rev_key, raw_url, sha256, rel_path, "ok")
                            prop_ok += 1
                            total_ok += 1
                            logger.info("      ✓ hash: %s…", sha256[:20])
                        else:
                            append_image_row(hid, rev_key, raw_url, None, None, "failed")
                            prop_fail += 1
                            total_failed += 1

                logger.info(
                    "  ✓ house_id=%s done — %d downloaded, %d failed",
                    hid, prop_ok, prop_fail
                )
                done_ids.add(str(hid))
                save_done(done_ids)
                time.sleep(BETWEEN_SLEEP)

        finally:
            try: page.close()
            except: pass
            try: ctx.close()
            except: pass
            try: browser.close()
            except: pass

    logger.info("=" * 64)
    logger.info("Done!")
    logger.info("  Total photo URLs found  : %d", total_urls)
    logger.info("  Successfully downloaded : %d", total_ok)
    logger.info("  Failed downloads        : %d", total_failed)
    logger.info("  No-photo properties     : %d  → image_no_reviews.json", len(no_images))
    logger.info("  CSV                     : %s", OUTPUT_CSV)
    logger.info("  Images folder           : %s", DOWNLOAD_DIR)
    logger.info("=" * 64)


if __name__ == "__main__":
    main()
