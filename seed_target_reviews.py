#!/usr/bin/env python3
"""
Seeds the scraped target property reviews into:
1. PostgreSQL table: new_house_reviews
2. PostgreSQL table: house_business_mappings (updates rating & review_count)
3. SQLite table: new_house_reviews (seeded_properties.db)
"""

import os
import sys
import csv
import json
import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("seed_target")

CURRENT_DIR = Path(__file__).resolve().parent
CSV_PATH = str(CURRENT_DIR / "target_property_reviews.csv")
SQLITE_PATH = str(CURRENT_DIR / "seeded_properties.db")

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

def main():
    if not os.path.exists(CSV_PATH):
        logger.error(f"CSV file not found: {CSV_PATH}")
        sys.exit(1)
        
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        reviews = list(reader)
        
    logger.info(f"Loaded {len(reviews)} reviews from {CSV_PATH}")
    if not reviews:
        logger.warning("No reviews found to seed.")
        return

    # 1. Seed into SQLite
    if os.path.exists(SQLITE_PATH):
        try:
            conn = sqlite3.connect(SQLITE_PATH)
            cur = conn.cursor()
            
            # Ensure table exists
            cur.execute("""
                CREATE TABLE IF NOT EXISTS new_house_reviews (
                    id TEXT PRIMARY KEY,
                    house_id TEXT NOT NULL,
                    user_id TEXT,
                    reviewer_name TEXT NOT NULL,
                    reviewer_email TEXT,
                    rating TEXT NOT NULL,
                    star_num TEXT NOT NULL,
                    comment TEXT NOT NULL,
                    school_name TEXT,
                    unit_name TEXT,
                    status TEXT NOT NULL,
                    source TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    deleted_at TEXT,
                    translated_comment TEXT
                );
            """)
            
            cur.execute("SELECT MAX(CAST(id AS INTEGER)) FROM new_house_reviews;")
            max_id_row = cur.fetchone()
            next_id = (max_id_row[0] or 0) + 1 if max_id_row and max_id_row[0] is not None else 1
            
            inserted_sqlite = 0
            for r in reviews:
                # Check duplicate by (house_id, reviewer_name, comment)
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
                    
            # Update SQLite house_business_mappings
            hids = list(set(int(r["house_id"]) for r in reviews))
            now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")
            for hid in hids:
                cur.execute("""
                    SELECT COUNT(*), AVG(CAST(rating AS FLOAT)) FROM new_house_reviews WHERE house_id = ?;
                """, (str(hid),))
                res = cur.fetchone()
                if res and res[0]:
                    cnt, avg_r = res
                    cur.execute("""
                        UPDATE house_business_mappings 
                        SET review_count = ?, rating = ?, updated_at = ? 
                        WHERE house_id = ?;
                    """, (str(cnt), f"{round(float(avg_r), 2):.2f}", now_iso, str(hid)))
            conn.commit()
            conn.close()
            logger.info(f"SQLite: successfully inserted {inserted_sqlite} new reviews and updated house_business_mappings in {SQLITE_PATH}.")
        except Exception as e:
            logger.error(f"Error seeding SQLite: {e}")

    # 2. Seed into PostgreSQL
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
        
        # Check existing to avoid duplicates
        hids = list(set(int(r["house_id"]) for r in reviews))
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
            logger.info(f"PostgreSQL: successfully inserted {len(to_insert)} new reviews into new_house_reviews.")
        else:
            logger.info("PostgreSQL: all reviews already exist in new_house_reviews, no duplicates inserted.")
            
        # Update house_business_mappings with counts & average ratings
        updated_mappings = 0
        for hid in hids:
            cur.execute("""
                SELECT COUNT(*), AVG(rating) FROM new_house_reviews WHERE house_id = %s;
            """, (hid,))
            cnt, avg_r = cur.fetchone()
            if cnt:
                cur.execute("""
                    UPDATE house_business_mappings 
                    SET review_count = %s, rating = %s, updated_at = NOW() 
                    WHERE house_id = %s;
                """, (cnt, round(float(avg_r), 2), hid))
                if cur.rowcount > 0:
                    updated_mappings += 1
        conn.commit()
        conn.close()
        logger.info(f"PostgreSQL: updated review_count & ratings for {updated_mappings} properties in house_business_mappings.")
    except Exception as e:
        logger.error(f"Error seeding PostgreSQL: {e}")

if __name__ == "__main__":
    main()
