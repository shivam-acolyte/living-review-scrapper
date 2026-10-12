# Property Business ID & Google Reviews Enrichment Pipeline

This pipeline connects to your **PostgreSQL** (or local **SQLite**) database, reads property records (`house_id`, `title`, `lat`, `lan`), queries Google Maps to find their official **Google Business IDs** (`place_id`, `cid`, `data_id`), metrics (`rating`, `review_count`), formatted address, and stores reviews in a separate `property_reviews` table.

---

## 1. Setup & Installation

Install dependencies:
```bash
pip install -r requirements.txt
```
*(Dependencies: `psycopg2-binary`, `requests`, `python-dotenv`)*

### Database Configuration
Copy the environment template:
```bash
cp .env.example .env
```

Edit `.env` with your PostgreSQL credentials:
```env
DB_TYPE=postgres
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
```

> **Note:** If PostgreSQL is not installed or running locally, the pipeline automatically provides a local SQLite database fallback (`property_pipeline.db`) so you can test and run immediately!

---

## 2. Database Schema

The pipeline automatically creates or updates the following tables:

### Properties Table (`properties`)
| Column | Type | Description |
|---|---|---|
| `house_id` | `VARCHAR(255)` | Primary Key (Your property identifier) |
| `title` | `TEXT` | Property or business name |
| `lat` | `DOUBLE PRECISION` | Latitude |
| `lan` | `DOUBLE PRECISION` | Longitude |
| `place_id` | `VARCHAR(255)` | Google Place ID (e.g. `ChIJF8niHmJFiEgRd4dgCJSPRC0`) |
| `cid` | `VARCHAR(255)` | Google Customer ID (e.g. `3261889896081557367`) |
| `data_id` | `VARCHAR(255)` | Google Hex ID (e.g. `0x488845621ee2c917:0x2d448f9408608777`) |
| `rating` | `NUMERIC(2,1)` | Google Star Rating (e.g. `4.7`) |
| `review_count`| `INTEGER` | Total Google Reviews Count |
| `scrape_status`| `VARCHAR(50)` | Status: `pending`, `completed`, or `not_found` |
| `last_scraped_at`| `TIMESTAMP` | Timestamp of enrichment |

### Reviews Table (`property_reviews`)
| Column | Type | Description |
|---|---|---|
| `id` | `SERIAL PRIMARY KEY` | Auto-incrementing review row ID |
| `house_id` | `VARCHAR(255)` | Foreign reference to `properties.house_id` |
| `place_id` | `VARCHAR(255)` | Google Place ID |
| `review_id` | `VARCHAR(255) UNIQUE`| Unique Google review ID |
| `author_name` | `TEXT` | Reviewer name |
| `rating` | `NUMERIC(2,1)` | Star rating given |
| `review_text` | `TEXT` | Customer review text |
| `published_at`| `TEXT` | Review relative date (e.g. "3 months ago") |
| `owner_reply` | `TEXT` | Official response from owner/management |
| `created_at` | `TIMESTAMP` | Insertion timestamp |

---

## 3. Running the Pipeline

### Step 1 (Optional): Seed sample test properties
Pre-populates sample test cases (e.g. Canvas Glasgow, Crystal Beach):
```bash
python seed_sample_data.py
```
To explicitly test with SQLite:
```bash
python seed_sample_data.py --db-type sqlite
```

### Step 2: Run the enrichment pipeline
```bash
python pipeline.py --limit 50 --delay 1.5 --max-reviews 10
```

CLI Options:
* `--limit`: Max properties to process per run (default: 50).
* `--delay`: Delay in seconds between requests to avoid rate limits (default: 1.5s).
* `--max-reviews`: Maximum reviews to save per property (default: 10).
* `--db-type`: Force database engine (`postgres` or `sqlite`).

---

## 4. Architecture & Scraper Engine

* **[scraper.py](file:///home/acolyte/.gemini/antigravity-ide/scratch/ne/property_gmaps_pipeline/scraper.py)**: Reverse-engineers Google Maps Protobuf search queries to retrieve official Place ID (`place_id`), Hex Data ID (`data_id`), and Customer ID (`cid`). Automatically computes Haversine distance to prioritize results nearest to the property coordinates.
* **[models.py](file:///home/acolyte/.gemini/antigravity-ide/scratch/ne/property_gmaps_pipeline/models.py)**: Dual-database storage layer supporting both PostgreSQL and SQLite schema migrations and upsert queries.
* **[pipeline.py](file:///home/acolyte/.gemini/antigravity-ide/scratch/ne/property_gmaps_pipeline/pipeline.py)**: Orchestrator that reads pending properties, queries Google Maps, updates the database, and stores reviews.
