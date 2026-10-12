import os
import sqlite3
from pathlib import Path

def _load_env_file():
    """Loads key-value pairs from .env into os.environ if not already set."""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        # Check parent directory as fallback
        env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                if k and k not in os.environ:
                    os.environ[k] = v

_load_env_file()

class Config:
    # Database mode: 'postgres' or 'sqlite'
    DB_TYPE = os.getenv("DB_TYPE", "postgres").lower()

    # PostgreSQL configuration
    DATABASE_URL = os.getenv("DATABASE_URL")
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = int(os.getenv("DB_PORT", "5432"))
    DB_NAME = os.getenv("DB_NAME", "postgres")
    DB_USER = os.getenv("DB_USER", "postgres")
    DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

    # SQLite fallback file
    SQLITE_PATH = os.getenv(
        "SQLITE_PATH",
        str(Path(__file__).resolve().parent / "property_pipeline.db")
    )

    # Table names
    PROPERTIES_TABLE = os.getenv("PROPERTIES_TABLE", "properties")
    REVIEWS_TABLE = os.getenv("REVIEWS_TABLE", "property_reviews")

    # Pipeline execution settings
    MAX_REVIEWS_PER_PROPERTY = int(os.getenv("MAX_REVIEWS_PER_PROPERTY", "10"))
    REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "1.5"))
    MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

    @classmethod
    def get_db_connection(cls, force_db_type=None):
        """
        Returns a database connection:
        - Tries PostgreSQL if DB_TYPE is 'postgres'
        - Falls back to SQLite if PostgreSQL fails or DB_TYPE is 'sqlite'
        """
        target_type = (force_db_type or cls.DB_TYPE).lower()

        if target_type == "sqlite":
            conn = sqlite3.connect(cls.SQLITE_PATH)
            conn.row_factory = sqlite3.Row
            return conn, "sqlite"

        # Attempt PostgreSQL
        try:
            import psycopg2
            if cls.DATABASE_URL:
                conn = psycopg2.connect(cls.DATABASE_URL)
            else:
                conn = psycopg2.connect(
                    host=cls.DB_HOST,
                    port=cls.DB_PORT,
                    dbname=cls.DB_NAME,
                    user=cls.DB_USER,
                    password=cls.DB_PASSWORD,
                    connect_timeout=3
                )
            return conn, "postgres"
        except Exception as pg_err:
            if target_type == "postgres" and not force_db_type:
                # Log and fallback gracefully to SQLite
                import logging
                logger = logging.getLogger("property_pipeline")
                logger.warning(
                    f"PostgreSQL connection to {cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME} failed ({pg_err}). "
                    f"Falling back to local SQLite database: {cls.SQLITE_PATH}"
                )
                conn = sqlite3.connect(cls.SQLITE_PATH)
                conn.row_factory = sqlite3.Row
                return conn, "sqlite"
            raise
