import csv
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

# Add pipeline directory to sys.path
PIPELINE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PIPELINE_DIR))

from scraper import GoogleMapsPropertyScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("pdf_generator")

CSV_SOURCE_PATH = "/home/acolyte/Downloads/acolye_living_v2_public_houses_2.csv"
OUTPUT_CSV_PATH = "/home/acolyte/Downloads/property_business_ids_first_100.csv"
OUTPUT_HTML_PATH = "/home/acolyte/Downloads/property_business_ids_report.html"
OUTPUT_PDF_PATH = "/home/acolyte/Downloads/property_business_ids_report.pdf"
CACHE_FILE = "/home/acolyte/Downloads/ne/property_gmaps_pipeline/enrichment_cache.json"

def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_cache(cache):
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def process_properties(limit=100):
    logger.info(f"Loading first {limit} properties from {CSV_SOURCE_PATH}...")
    scraper = GoogleMapsPropertyScraper(max_retries=2, timeout=10)
    cache = load_cache()

    processed_rows = []
    scraped_count = 0
    pre_existing_count = 0

    with open(CSV_SOURCE_PATH, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.reader(f)
        for i, row in enumerate(reader):
            if i >= limit:
                break
            if len(row) < 4:
                continue

            house_id = row[0].strip()
            title = row[1].strip()
            lat = row[2].strip()
            lan = row[3].strip()

            meta = {}
            if len(row) > 4 and row[4].strip():
                try:
                    meta = json.loads(row[4])
                except Exception:
                    pass

            existing_place_id = meta.get("place_id") or ""
            addr = meta.get("address") or ""

            place_id = existing_place_id
            cid = ""
            data_id = ""
            rating = ""
            review_count = ""
            source_status = "Pre-existing"

            # Check cache first
            if house_id in cache:
                cached = cache[house_id]
                place_id = cached.get("place_id", place_id)
                cid = cached.get("cid", "")
                data_id = cached.get("data_id", "")
                rating = cached.get("rating", "")
                review_count = cached.get("review_count", "")
                source_status = cached.get("status", source_status)
                if not addr:
                    addr = cached.get("address", "")
            elif not place_id:
                # Query Google Maps live
                query_name = addr if addr and len(addr) > 5 else title
                logger.info(f"[{i+1}/{limit}] Live Scraping: '{query_name}' ({lat}, {lan}) for house_id: {house_id}")
                
                # Try with full address or title
                search_res = scraper.search_property(query_name, lat=lat, lan=lan)
                if not search_res and query_name != title:
                    search_res = scraper.search_property(title, lat=lat, lan=lan)

                if search_res and search_res.get("place_id"):
                    place_id = search_res.get("place_id") or ""
                    cid = search_res.get("cid") or ""
                    data_id = search_res.get("data_id") or ""
                    rating = str(search_res.get("rating") or "")
                    review_count = str(search_res.get("review_count") or "")
                    source_status = "Live Enriched"
                    scraped_count += 1
                    if not addr:
                        addr = search_res.get("address") or ""
                else:
                    source_status = "Not Found"
                    logger.warning(f"  ✗ Could not match Google Place for house_id: {house_id}")

                cache[house_id] = {
                    "place_id": place_id,
                    "cid": cid,
                    "data_id": data_id,
                    "rating": rating,
                    "review_count": review_count,
                    "address": addr,
                    "status": source_status
                }
                save_cache(cache)
                time.sleep(0.4)
            else:
                pre_existing_count += 1

            processed_rows.append({
                "index": i + 1,
                "house_id": house_id,
                "title": title,
                "place_id": place_id or "N/A",
                "cid": cid or "N/A",
                "data_id": data_id or "N/A",
                "lat": lat,
                "lan": lan,
                "address": addr or "N/A",
                "status": source_status
            })

    logger.info(
        f"Processed {len(processed_rows)} rows: {pre_existing_count} pre-existing Place IDs, "
        f"{scraped_count} live enriched via Google Maps."
    )
    return processed_rows

def export_csv(rows):
    with open(OUTPUT_CSV_PATH, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "index", "house_id", "title", "place_id", "cid", "data_id", "latitude", "longitude", "address", "status"
        ])
        for r in rows:
            writer.writerow([
                r["index"], r["house_id"], r["title"], r["place_id"], r["cid"],
                r["data_id"], r["lat"], r["lan"], r["address"], r["status"]
            ])
    logger.info(f"Enriched CSV exported to {OUTPUT_CSV_PATH}")

def generate_html_report(rows):
    total = len(rows)
    with_place_id = sum(1 for r in rows if r["place_id"] != "N/A")
    match_rate = f"{(with_place_id / total * 100):.1f}%" if total else "0%"
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
    display: flex;
    justify-content: space-between;
    align-items: flex-end;
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
    <span class="stat-label">Total Sampled:</span>
    <span class="stat-val">{total}</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">Identified Places:</span>
    <span class="stat-val">{with_place_id}</span>
  </div>
  <div class="stat-card">
    <span class="stat-label">Coverage:</span>
    <span class="stat-val">{match_rate}</span>
  </div>
</div>

<table>
  <thead>
    <tr>
      <th style="width: 3%;">#</th>
      <th style="width: 8%;">House ID</th>
      <th style="width: 22%;">Property Title</th>
      <th style="width: 25%;">Google Place ID</th>
      <th style="width: 15%;">CID / Data ID</th>
      <th style="width: 15%;">Coordinates (Lat, Lon)</th>
      <th style="width: 12%;">Status</th>
    </tr>
  </thead>
  <tbody>
"""
    for r in rows:
        status_cls = "badge-existing"
        if r["status"] == "Live Enriched":
            status_cls = "badge-enriched"
        elif r["status"] == "Not Found":
            status_cls = "badge-missing"

        cid_display = r["cid"]
        if cid_display == "N/A" and r["data_id"] != "N/A":
            cid_display = r["data_id"]

        html += f"""    <tr>
      <td>{r["index"]}</td>
      <td class="mono"><strong>{r["house_id"]}</strong></td>
      <td><strong>{r["title"]}</strong><br><span style="color: #64748b; font-size: 6pt;">{r["address"][:55]}</span></td>
      <td class="mono">{r["place_id"]}</td>
      <td class="mono">{cid_display}</td>
      <td class="mono">{r["lat"]}, {r["lan"]}</td>
      <td><span class="badge {status_cls}">{r["status"]}</span></td>
    </tr>\n"""

    html += """  </tbody>
</table>

</body>
</html>"""

    with open(OUTPUT_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    logger.info(f"Report HTML template written to {OUTPUT_HTML_PATH}")

def convert_html_to_pdf():
    logger.info(f"Converting HTML to PDF via LibreOffice headless...")
    cmd = [
        "libreoffice",
        "--headless",
        "--convert-to",
        "pdf",
        OUTPUT_HTML_PATH,
        "--outdir",
        "/home/acolyte/Downloads"
    ]
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if res.returncode == 0 and os.path.exists(OUTPUT_PDF_PATH):
        file_size = os.path.getsize(OUTPUT_PDF_PATH)
        logger.info(f"✓ PDF successfully generated: {OUTPUT_PDF_PATH} ({file_size} bytes)")
        return OUTPUT_PDF_PATH
    else:
        logger.error(f"LibreOffice PDF conversion failed: {res.stderr}")
        return None

def main():
    rows = process_properties(limit=100)
    export_csv(rows)
    generate_html_report(rows)
    pdf_path = convert_html_to_pdf()
    if pdf_path:
        print("\n" + "=" * 65)
        print("REPORT GENERATION COMPLETE")
        print(f"PDF Location: {pdf_path}")
        print(f"CSV Location: {OUTPUT_CSV_PATH}")
        print("=" * 65)

if __name__ == "__main__":
    main()
