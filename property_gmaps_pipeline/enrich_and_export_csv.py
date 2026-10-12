#!/usr/bin/env python3
"""
Full Dataset Property Google Maps Enrichment Pipeline.

Enriches properties from source CSV with:
- Google Place ID (e.g. ChIJq5v4plb1bkgRDmoTbBffZTw)
- Google Hex Hash (e.g. 0x486ef556a6f89bab:0x3c65df176c136a0e)
- Google Business / Customer ID (CID) (e.g. 4352129906613905934)
- Formatted Address, Rating, and Review Count

Features:
- Instant mathematical conversion for pre-existing Place IDs (sub-second)
- Multi-worker concurrent scraping for missing listings
- Thread-safe disk-backed caching (fully resumable)
- Continuous CSV flushing every N items
- Progress tracking with ETA
"""

import argparse
import csv
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PIPELINE_DIR))

from scraper import (
    GoogleMapsPropertyScraper,
    place_id_to_hex_hash,
    hex_hash_to_cid,
    hex_hash_to_place_id
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("enricher")

DEFAULT_INPUT_CSV = "/home/acolyte/Downloads/acolye_living_v2_public_houses_2.csv"
DEFAULT_OUTPUT_CSV = "/home/acolyte/Downloads/property_business_ids.csv"
OUTPUT_CSV_NE = "/home/acolyte/Downloads/ne/property_business_ids.csv"
OUTPUT_HTML_PATH = "/home/acolyte/Downloads/property_business_ids_report.html"
OUTPUT_PDF_PATH = "/home/acolyte/Downloads/property_business_ids_report.pdf"
CACHE_FILE = PIPELINE_DIR / "enrichment_cache.json"

cache_lock = threading.Lock()
csv_lock = threading.Lock()
stop_requested = False

def handle_sigint(signum, frame):
    global stop_requested
    logger.warning("\n[!] Graceful stop requested (Ctrl+C). Saving all progress...")
    stop_requested = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load cache: {e}")
            return {}
    return {}

def save_cache_to_disk(cache_dict):
    with cache_lock:
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(cache_dict, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.warning(f"Error saving cache: {e}")

def flush_csv(rows, output_path):
    with csv_lock:
        fieldnames = [
            "index", "house_id", "title", "hex_hash", "place_id", "cid", "data_id",
            "latitude", "longitude", "address", "rating", "review_count", "status"
        ]
        sorted_rows = sorted(rows, key=lambda x: x["index"])
        for p in [output_path, OUTPUT_CSV_NE]:
            try:
                os.makedirs(os.path.dirname(p), exist_ok=True)
                with open(p, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(sorted_rows)
            except Exception as e:
                logger.warning(f"Error writing to {p}: {e}")

def enrich_single_item(item_tuple, scraper, cache, delay=0.15):
    """
    Enriches a single property item:
    1. Checks cache
    2. If place_id exists in meta, converts mathematically to hex_hash and cid (instant)
    3. If missing, queries Google Maps
    """
    idx, raw = item_tuple
    hid = raw[0].strip()
    title = raw[1].strip()
    lat = raw[2].strip() if len(raw) > 2 else ""
    lan = raw[3].strip() if len(raw) > 3 else ""

    meta = {}
    if len(raw) > 4 and raw[4].strip():
        try:
            meta = json.loads(raw[4])
        except Exception:
            pass

    existing_place_id = meta.get("place_id") or ""
    existing_addr = meta.get("address") or ""

    with cache_lock:
        cached = cache.get(hid, {})

    place_id = cached.get("place_id") or existing_place_id
    cid = cached.get("cid") or ""
    data_id = cached.get("data_id") or ""
    rating = cached.get("rating") or ""
    review_count = cached.get("review_count") or ""
    addr = cached.get("address") or existing_addr
    status = cached.get("status") or ("Pre-existing" if existing_place_id else "Pending")

    # Step 1: Already cached with CID and Place ID
    if cid and data_id and place_id and cid != "N/A" and data_id != "N/A" and place_id != "N/A":
        return {
            "index": idx,
            "house_id": hid,
            "title": title,
            "hex_hash": data_id,
            "place_id": place_id,
            "cid": cid,
            "data_id": data_id,
            "latitude": lat,
            "longitude": lan,
            "address": addr or "N/A",
            "rating": rating or "N/A",
            "review_count": review_count or "0",
            "status": status,
            "from_cache": True
        }

    # Step 2: Instant Mathematical Conversion if Place ID is present
    if place_id and place_id.startswith("ChIJ"):
        calc_hex = place_id_to_hex_hash(place_id)
        calc_cid = hex_hash_to_cid(calc_hex) if calc_hex else None
        if calc_hex and calc_cid:
            data_id = calc_hex
            cid = calc_cid
            status = "Pre-existing (Resolved)"
            with cache_lock:
                cache[hid] = {
                    "place_id": place_id,
                    "cid": cid,
                    "data_id": data_id,
                    "rating": rating or "N/A",
                    "review_count": review_count or "0",
                    "address": addr or "N/A",
                    "status": status
                }
            return {
                "index": idx,
                "house_id": hid,
                "title": title,
                "hex_hash": data_id,
                "place_id": place_id,
                "cid": cid,
                "data_id": data_id,
                "latitude": lat,
                "longitude": lan,
                "address": addr or "N/A",
                "rating": rating or "N/A",
                "review_count": review_count or "0",
                "status": status,
                "from_cache": True
            }

    # Step 3: Query Google Maps for listings without Place ID
    query = addr if addr and len(addr) > 5 else title
    res = scraper.search_property(query, lat=lat, lan=lan)
    if not res and query != title:
        res = scraper.search_property(title, lat=lat, lan=lan)

    if res and res.get("place_id"):
        place_id = res.get("place_id") or place_id
        cid = res.get("cid") or cid
        data_id = res.get("data_id") or data_id
        rating = str(res.get("rating") or rating)
        review_count = str(res.get("review_count") or review_count)
        status = "Live Enriched"
        if not addr:
            addr = res.get("address") or ""
    elif res and res.get("hex_hash"):
        data_id = res.get("hex_hash")
        place_id = hex_hash_to_place_id(data_id) or "N/A"
        cid = res.get("cid") or hex_hash_to_cid(data_id)
        status = "Live Enriched"
    else:
        status = "Not Found on GMaps"

    updated_entry = {
        "place_id": place_id or "N/A",
        "cid": cid or "N/A",
        "data_id": data_id or "N/A",
        "rating": rating or "N/A",
        "review_count": review_count or "0",
        "address": addr or "N/A",
        "status": status
    }

    with cache_lock:
        cache[hid] = updated_entry

    if delay > 0:
        time.sleep(delay)

    return {
        "index": idx,
        "house_id": hid,
        "title": title,
        "hex_hash": updated_entry["data_id"],
        "place_id": updated_entry["place_id"],
        "cid": updated_entry["cid"],
        "data_id": updated_entry["data_id"],
        "latitude": lat,
        "longitude": lan,
        "address": updated_entry["address"],
        "rating": updated_entry["rating"],
        "review_count": updated_entry["review_count"],
        "status": updated_entry["status"],
        "from_cache": False
    }

def generate_html_report(rows, html_path):
    total = len(rows)
    with_place_id = sum(1 for r in rows if r["place_id"] != "N/A")
    with_cid = sum(1 for r in rows if r["cid"] != "N/A")
    date_str = time.strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Property Google Business ID Report</title>
<style>
  @page {{
    size: A4 landscape;
    margin: 10mm 8mm 12mm 8mm;
    @bottom-right {{
      content: "Page " counter(page) " of " counter(pages);
      font-size: 8pt;
      color: #64748b;
    }}
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    color: #1e293b;
    margin: 0;
    padding: 0;
    font-size: 8pt;
    line-height: 1.3;
    background-color: #fff;
  }}
  .header {{
    border-bottom: 2px solid #2563eb;
    padding-bottom: 6px;
    margin-bottom: 10px;
  }}
  .title-block h1 {{
    font-size: 15pt;
    font-weight: 700;
    color: #0f172a;
    margin: 0 0 3px 0;
  }}
  .title-block p {{
    font-size: 8pt;
    color: #64748b;
    margin: 0;
  }}
  .stats-bar {{
    margin-bottom: 10px;
  }}
  .stat-card {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 4px;
    padding: 4px 10px;
    display: inline-block;
    margin-right: 12px;
  }}
  .stat-label {{
    font-size: 6.5pt;
    text-transform: uppercase;
    color: #64748b;
    font-weight: 600;
  }}
  .stat-val {{
    font-size: 10pt;
    font-weight: 700;
    color: #0284c7;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin-top: 4px;
  }}
  th {{
    background-color: #0f172a;
    color: #ffffff;
    font-size: 7.5pt;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.4px;
    padding: 5px 6px;
    text-align: left;
    border: 1px solid #0f172a;
  }}
  td {{
    padding: 4px 6px;
    border: 1px solid #cbd5e1;
    font-size: 7pt;
    vertical-align: top;
    word-break: break-word;
  }}
  tr:nth-child(even) {{
    background-color: #f8fafc;
  }}
  .mono {{
    font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
    font-size: 6.5pt;
    color: #0369a1;
  }}
  .badge {{
    display: inline-block;
    padding: 2px 5px;
    border-radius: 3px;
    font-size: 6pt;
    font-weight: 600;
    text-transform: uppercase;
  }}
  .badge-enriched {{
    background: #dcfce7;
    color: #15803d;
  }}
  .badge-existing {{
    background: #e0f2fe;
    color: #0369a1;
  }}
  .badge-missing {{
    background: #fee2e2;
    color: #b91c1c;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="title-block">
    <h1>Google Business ID & Place Identification Report</h1>
    <p>Source Dataset: <code>acolye_living_v2_public_houses_2.csv</code> | Generated: {date_str}</p>
  </div>
</div>

<div class="stats-bar">
  <div class="stat-card">
    <span class="stat-label">Total Properties:</span>
    <span class="stat-val">{total}</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">Place IDs Identified:</span>
    <span class="stat-val">{with_place_id} / {total}</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">Hex Hashes & CIDs Resolved:</span>
    <span class="stat-val">{with_cid} / {total}</span>
  </div>
</div>

<table>
  <thead>
    <tr>
      <th style="width: 3%;">#</th>
      <th style="width: 8%;">House ID</th>
      <th style="width: 20%;">Property Title</th>
      <th style="width: 22%;">Hex Hash (data_id)</th>
      <th style="width: 23%;">Google Place ID</th>
      <th style="width: 14%;">Customer ID (CID)</th>
      <th style="width: 10%;">Status</th>
    </tr>
  </thead>
  <tbody>
"""
    for r in rows:
        status_cls = "badge-enriched" if "Resolved" in r["status"] or "Enriched" in r["status"] else "badge-existing"
        if r["place_id"] == "N/A":
            status_cls = "badge-missing"

        html += f"""    <tr>
      <td>{r["index"]}</td>
      <td class="mono"><strong>{r["house_id"]}</strong></td>
      <td><strong>{r["title"]}</strong><br><span style="color: #64748b; font-size: 6pt;">{r["address"][:55]}</span></td>
      <td class="mono">{r["hex_hash"]}</td>
      <td class="mono">{r["place_id"]}</td>
      <td class="mono">{r["cid"]}</td>
      <td><span class="badge {status_cls}">{r["status"]}</span></td>
    </tr>\n"""

    html += """  </tbody>
</table>

</body>
</html>"""

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

def convert_html_to_pdf(html_path, pdf_path):
    cmd = [
        "libreoffice",
        "--headless",
        "--convert-to",
        "pdf",
        html_path,
        "--outdir",
        os.path.dirname(pdf_path)
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode == 0 and os.path.exists(pdf_path):
        file_size = os.path.getsize(pdf_path)
        logger.info(f"✓ PDF report successfully compiled: {pdf_path} ({file_size} bytes)")
    else:
        logger.warning(f"Could not convert HTML to PDF: {res.stderr}")

def main():
    parser = argparse.ArgumentParser(description="Enrich CSV properties with Google Place IDs, Hex Hashes, and CIDs")
    parser.add_argument("--all", action="store_true", help="Process ALL rows from source CSV (12,000+ items)")
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of rows to process (default: all if --all, else 100)")
    parser.add_argument("--offset", type=int, default=0, help="Skip first N rows (default: 0)")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent scraping workers (default: 4)")
    parser.add_argument("--delay", type=float, default=0.15, help="Polite delay between Google Maps queries in seconds (default: 0.15)")
    parser.add_argument("--save-interval", type=int, default=50, help="Flush CSV and cache every N items (default: 50)")
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV path")
    parser.add_argument("--output", default=DEFAULT_OUTPUT_CSV, help="Output CSV path")
    parser.add_argument("--pdf", action="store_true", help="Generate PDF report upon completion (recommended only for <= 500 rows)")
    parser.add_argument("--no-pdf", action="store_true", help="Explicitly disable PDF report generation")

    args = parser.parse_args()

    limit = None if args.all else (args.limit if args.limit is not None else 100)
    logger.info("=" * 65)
    logger.info("Google Maps Property ID Enrichment Pipeline")
    logger.info(f"• Input CSV     : {args.input}")
    logger.info(f"• Output CSV    : {args.output}")
    logger.info(f"• Mode          : {'ALL ROWS (Full Dataset - 12,131 properties)' if args.all else f'Limit {limit} rows'}")
    logger.info(f"• Concurrency   : {args.workers} worker threads")
    logger.info(f"• Save Interval : Every {args.save_interval} items")
    logger.info("=" * 65)

    cache = load_cache()
    logger.info(f"Loaded {len(cache)} cached property entries from cache file.")

    # Read input CSV
    raw_items = []
    with open(args.input, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for i, r in enumerate(reader):
            if i < args.offset:
                continue
            if limit is not None and len(raw_items) >= limit:
                break
            if len(r) < 4:
                continue
            raw_items.append((i + 1, r))

    total_to_process = len(raw_items)
    logger.info(f"Loaded {total_to_process} properties to process.")

    scraper = GoogleMapsPropertyScraper(max_retries=2, timeout=10)
    completed_rows = []
    processed_count = 0
    cached_count = 0
    live_resolved = 0
    start_time = time.time()

    logger.info("Processing properties with instant resolver & worker pool...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_map = {
            executor.submit(enrich_single_item, item, scraper, cache, args.delay): item
            for item in raw_items
        }

        for future in as_completed(future_map):
            if stop_requested:
                logger.warning("Aborting pending tasks due to stop request...")
                break

            try:
                res = future.result()
                completed_rows.append(res)
                processed_count += 1

                if res.get("from_cache"):
                    cached_count += 1
                else:
                    live_resolved += 1

                # Periodic save & progress reporting
                if processed_count % args.save_interval == 0 or processed_count == total_to_process:
                    save_cache_to_disk(cache)
                    flush_csv(completed_rows, args.output)

                    elapsed = time.time() - start_time
                    rate = processed_count / elapsed if elapsed > 0 else 0
                    remaining = (total_to_process - processed_count) / rate if rate > 0 else 0
                    rem_min = int(remaining // 60)
                    rem_sec = int(remaining % 60)

                    pct = (processed_count / total_to_process) * 100
                    logger.info(
                        f"[{processed_count}/{total_to_process} | {pct:.1f}%] "
                        f"Speed: {rate:.1f} items/s | ETA: {rem_min}m {rem_sec}s | "
                        f"Instant/Cached: {cached_count} | Live Enriched: {live_resolved}"
                    )
            except Exception as e:
                logger.error(f"Error processing property: {e}")

    # Final save
    save_cache_to_disk(cache)
    flush_csv(completed_rows, args.output)
    total_elapsed = time.time() - start_time
    logger.info("=" * 65)
    logger.info(f"✓ Completed {len(completed_rows)}/{total_to_process} properties in {total_elapsed:.1f}s")
    logger.info(f"✓ Final CSV: {args.output} ({len(completed_rows)} records)")

    # Generate PDF if requested or small batch
    should_pdf = args.pdf or (not args.no_pdf and total_to_process <= 500)
    if should_pdf:
        logger.info("Generating PDF report...")
        generate_html_report(sorted(completed_rows, key=lambda x: x["index"]), OUTPUT_HTML_PATH)
        convert_html_to_pdf(OUTPUT_HTML_PATH, OUTPUT_PDF_PATH)
    else:
        logger.info(f"Skipping PDF compilation (Dataset size: {total_to_process} rows. PDF is recommended for <= 500 rows).")

    logger.info("Done.")

if __name__ == "__main__":
    main()
