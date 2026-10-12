import logging
from datetime import datetime

logger = logging.getLogger("property_pipeline")

def init_tables(conn, db_type="postgres", properties_table="properties", reviews_table="property_reviews"):
    """
    Initializes tables:
    1. Base properties table (house_id, title, lat, lan, place_id, cid, data_id, rating, review_count, scrape_status)
    2. Separate reviews table linked by house_id / place_id
    """
    with conn.cursor() if db_type == "postgres" else conn as cur:
        if db_type == "postgres":
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {properties_table} (
                    house_id VARCHAR(255) PRIMARY KEY,
                    title TEXT NOT NULL,
                    lat DOUBLE PRECISION NOT NULL,
                    lan DOUBLE PRECISION NOT NULL,
                    place_id VARCHAR(255),
                    cid VARCHAR(255),
                    data_id VARCHAR(255),
                    rating NUMERIC(2,1),
                    review_count INTEGER DEFAULT 0,
                    scrape_status VARCHAR(50) DEFAULT 'pending',
                    last_scraped_at TIMESTAMP
                );
            """)

            # Ensure columns exist in case table was pre-existing with only house_id/title/lat/lan
            columns_to_ensure = [
                ("place_id", "VARCHAR(255)"),
                ("cid", "VARCHAR(255)"),
                ("data_id", "VARCHAR(255)"),
                ("rating", "NUMERIC(2,1)"),
                ("review_count", "INTEGER DEFAULT 0"),
                ("scrape_status", "VARCHAR(50) DEFAULT 'pending'"),
                ("last_scraped_at", "TIMESTAMP")
            ]
            for col_name, col_type in columns_to_ensure:
                cur.execute(f"""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns 
                            WHERE table_name = '{properties_table}' AND column_name = '{col_name}'
                        ) THEN
                            ALTER TABLE {properties_table} ADD COLUMN {col_name} {col_type};
                        END IF;
                    END $$;
                """)

            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {reviews_table} (
                    id SERIAL PRIMARY KEY,
                    house_id VARCHAR(255) NOT NULL,
                    place_id VARCHAR(255) NOT NULL,
                    review_id VARCHAR(255) UNIQUE NOT NULL,
                    author_name TEXT,
                    rating NUMERIC(2,1),
                    review_text TEXT,
                    published_at TEXT,
                    owner_reply TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_{reviews_table}_house_id ON {reviews_table}(house_id);
                CREATE INDEX IF NOT EXISTS idx_{reviews_table}_place_id ON {reviews_table}(place_id);
            """)
            conn.commit()
        else:
            # SQLite DDL
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {properties_table} (
                    house_id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    lat REAL NOT NULL,
                    lan REAL NOT NULL,
                    place_id TEXT,
                    cid TEXT,
                    data_id TEXT,
                    rating REAL,
                    review_count INTEGER DEFAULT 0,
                    scrape_status TEXT DEFAULT 'pending',
                    last_scraped_at DATETIME
                );
            """)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {reviews_table} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    house_id TEXT NOT NULL,
                    place_id TEXT NOT NULL,
                    review_id TEXT UNIQUE NOT NULL,
                    author_name TEXT,
                    rating REAL,
                    review_text TEXT,
                    published_at TEXT,
                    owner_reply TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{reviews_table}_house_id ON {reviews_table}(house_id);")
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{reviews_table}_place_id ON {reviews_table}(place_id);")
            conn.commit()

        logger.info(f"Database tables verified/initialized ({properties_table}, {reviews_table}) [{db_type}].")

def fetch_pending_properties(conn, db_type="postgres", properties_table="properties", limit=50):
    """Fetches properties pending Google Business ID & review enrichment."""
    query = f"""
        SELECT house_id, title, lat, lan 
        FROM {properties_table}
        WHERE place_id IS NULL OR scrape_status = 'pending'
        ORDER BY house_id
        LIMIT {"%s" if db_type == "postgres" else "?"};
    """
    if db_type == "postgres":
        from psycopg2.extras import RealDictCursor
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, (limit,))
            return [dict(r) for r in cur.fetchall()]
    else:
        cursor = conn.cursor()
        cursor.execute(query, (limit,))
        return [dict(r) for r in cursor.fetchall()]

def update_property_status(conn, db_type="postgres", properties_table="properties",
                           house_id=None, place_id=None, cid=None, data_id=None,
                           rating=None, review_count=0, status="completed"):
    """Updates property with Google Place IDs, rating, and status."""
    ph = "%s" if db_type == "postgres" else "?"
    ts_expr = "CURRENT_TIMESTAMP"
    query = f"""
        UPDATE {properties_table}
        SET place_id = {ph},
            cid = {ph},
            data_id = {ph},
            rating = {ph},
            review_count = {ph},
            scrape_status = {ph},
            last_scraped_at = {ts_expr}
        WHERE house_id = {ph};
    """
    params = (place_id, cid, data_id, rating, review_count, status, house_id)
    if db_type == "postgres":
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()
    else:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()

def save_reviews(conn, db_type="postgres", reviews_table="property_reviews",
                 house_id=None, place_id=None, reviews=None):
    """Inserts or updates reviews associated with a property."""
    if not reviews:
        return 0
    saved = 0
    ph = "%s" if db_type == "postgres" else "?"
    query = f"""
        INSERT INTO {reviews_table} (
            house_id, place_id, review_id, author_name, rating, review_text, published_at, owner_reply
        ) VALUES ({ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph}, {ph})
        ON CONFLICT (review_id) DO UPDATE SET
            rating = EXCLUDED.rating,
            review_text = EXCLUDED.review_text,
            published_at = EXCLUDED.published_at,
            owner_reply = EXCLUDED.owner_reply;
    """
    if db_type == "postgres":
        with conn.cursor() as cur:
            for r in reviews:
                cur.execute(query, (
                    house_id,
                    place_id,
                    r.get("review_id"),
                    r.get("author_name"),
                    r.get("rating"),
                    r.get("review_text"),
                    r.get("published_at"),
                    r.get("owner_reply")
                ))
                saved += 1
        conn.commit()
    else:
        cur = conn.cursor()
        for r in reviews:
            cur.execute(query, (
                house_id,
                place_id,
                r.get("review_id"),
                r.get("author_name"),
                r.get("rating"),
                r.get("review_text"),
                r.get("published_at"),
                r.get("owner_reply")
            ))
            saved += 1
        conn.commit()
    return saved
