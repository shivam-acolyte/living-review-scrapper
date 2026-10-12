import argparse
import logging
import sys
from config import Config
from models import init_tables

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("seed_data")

SAMPLE_PROPERTIES = [
    {
        "house_id": "HOUSE-001",
        "title": "Canvas Glasgow",
        "lat": 55.865582,
        "lan": -4.250567
    },
    {
        "house_id": "HOUSE-002",
        "title": "Crystal Beach Community Park",
        "lat": 45.351875,
        "lan": -75.839420
    },
    {
        "house_id": "HOUSE-003",
        "title": "Crystal Beach Lakeview Community Association",
        "lat": 45.349846,
        "lan": -75.833025
    }
]

def seed_database(db_type_override=None):
    try:
        conn, db_type = Config.get_db_connection(force_db_type=db_type_override)
        logger.info(f"Connected to database [engine: {db_type}].")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)

    try:
        # Initialize tables
        init_tables(conn, db_type=db_type,
                    properties_table=Config.PROPERTIES_TABLE,
                    reviews_table=Config.REVIEWS_TABLE)

        ph = "%s" if db_type == "postgres" else "?"
        query = f"""
            INSERT INTO {Config.PROPERTIES_TABLE} (house_id, title, lat, lan, scrape_status)
            VALUES ({ph}, {ph}, {ph}, {ph}, 'pending')
            ON CONFLICT (house_id) DO UPDATE SET
                title = EXCLUDED.title,
                lat = EXCLUDED.lat,
                lan = EXCLUDED.lan,
                scrape_status = 'pending';
        """

        if db_type == "postgres":
            with conn.cursor() as cur:
                for p in SAMPLE_PROPERTIES:
                    cur.execute(query, (p["house_id"], p["title"], p["lat"], p["lan"]))
            conn.commit()
        else:
            cur = conn.cursor()
            for p in SAMPLE_PROPERTIES:
                cur.execute(query, (p["house_id"], p["title"], p["lat"], p["lan"]))
            conn.commit()

        logger.info(f"Successfully seeded {len(SAMPLE_PROPERTIES)} properties into '{Config.PROPERTIES_TABLE}' [{db_type}].")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed sample properties for Google Maps Pipeline")
    parser.add_argument("--db-type", type=str, default=None, choices=["postgres", "sqlite"], help="Database type (postgres/sqlite)")
    args = parser.parse_args()
    seed_database(db_type_override=args.db_type)
