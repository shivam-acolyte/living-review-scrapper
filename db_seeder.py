#!/usr/bin/env python3
"""
Database Seeding Utility for Property Business IDs and Reviews.

Provides a reusable, throttled utility function `seed_csv_to_db` where you can
directly bind database columns to CSV headers, supporting:
- PostgreSQL (via psycopg2) and SQLite (via sqlite3)
- Slow, controlled writing with configurable batch size and delay (throttling)
- Automatic loading of .env credentials
- Direct column mapping: {"db_column": "csv_header"} or custom callables or constants
- Constants: ("const", "approved") or "approved", and null/None
- Modes: 'upsert' (ON CONFLICT DO UPDATE), 'insert', and 'update'
- Auto-cleaning of 'N/A', empty strings, and null representations
- Automatic schema validation against existing database tables
- High-efficiency pipelining via psycopg2.extras.execute_batch
- Real-time per-batch progress logging
- Graceful stop on Ctrl+C (SIGINT)
- Direct SQL script generation via --export-sql
- CLI usage: --reviews, --business-ids, --all, --dry-run, --batch-size, --delay
"""

import os
import sys
import csv
import json
import time
import signal
import argparse
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Callable, Union, Optional, Tuple, Set

# Alias null to None for developer convenience in dictionary definitions
null = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("db_seeder")

# Default file paths
CURRENT_DIR = Path(__file__).resolve().parent
DEFAULT_REVIEWS_CSV = str(CURRENT_DIR / "property_reviews.csv")
DEFAULT_BUSINESS_IDS_CSV = str(CURRENT_DIR / "property_business_ids.csv")
DEFAULT_SQLITE_DB = str(CURRENT_DIR / "seeded_properties.db")

# Graceful stop flag
stop_requested = False

def handle_sigint(signum, frame):
    global stop_requested
    logger.warning("\n[!] Graceful stop requested (Ctrl+C). Finishing current batch and saving...")
    stop_requested = True

signal.signal(signal.SIGINT, handle_sigint)
signal.signal(signal.SIGTERM, handle_sigint)

def load_env_file():
    """Loads key-value pairs from .env into os.environ without requiring external packages."""
    env_path = CURRENT_DIR / ".env"
    if not env_path.exists():
        env_path = CURRENT_DIR.parent / ".env"
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

load_env_file()

def resolve_rating(row: Dict[str, str]) -> str:
    r = row.get("rating")
    if r and r not in ("", "N/A", "null", "None"):
        try:
            val = float(r)
            if 1.0 <= val <= 5.0:
                return f"{val:.2f}"
        except Exception:
            pass
    return "5.00"

def resolve_star_num(row: Dict[str, str]) -> int:
    s = row.get("star_num")
    if s and s not in ("", "N/A", "null", "None"):
        try:
            val = int(round(float(s)))
            if 1 <= val <= 5:
                return val
        except Exception:
            pass
    r = row.get("rating")
    if r and r not in ("", "N/A", "null", "None"):
        try:
            return max(1, min(5, int(round(float(r)))))
        except Exception:
            pass
    return 5

def now_utc_str() -> str:
    """Returns the current UTC timestamp formatted for database storage."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S+00")

# ---------------------------------------------------------------------------
# Direct Column-to-CSV-Header Bindings
# ---------------------------------------------------------------------------

new_house_reviews = {
    # Database Column       : CSV Header Name / Callable / Constant
    "id": "id",
    "house_id": "house_id",
    "user_id": "user_id",
    "reviewer_name": "reviewer_name",
    "reviewer_email": "reviewer_email",
    "rating": resolve_rating,
    "star_num": resolve_star_num,
    "comment": lambda row: (row.get("comment") or "").strip(),  # Comment is NOT NULL in database
    "images": "images",                                         # Review images (binds if present in CSV, else NULL)
    "school_name": "school_name",
    "unit_name": "unit_name",
    "status": ("const", "approved"),                            # Must be: 'pending', 'approved', 'rejected', 'deleted'
    "source": ("const", "google_review"),                       # Must match DB constraint: 'google_review' or 'User'
    "created_at": "created_at",
    "updated_at": "updated_at",
    "deleted_at": "deleted_at",
    "translated_comment": null,                                 # Translated comment is null (None)
}

house_business_mappings = {
    # Database Column       : CSV Header Name / Callable / Constant
    "house_id": "house_id",
    "place_id": "place_id",
    "business_id": "cid",            # Google Business / Customer ID (CID)
    "business_name": "title",        # Business / Listing title
    "business_address": "address",   # Formatted full address
    "match_status": "status",        # Match status (Fully Resolved, Live Enriched)
    "hex_hash": "hex_hash",          # Binds if column exists in table
    "address": "address",            # Binds if column exists in table
    "rating": resolve_rating,
    "review_count": "review_count",
    "last_synced_at": lambda row: now_utc_str(),
    "created_at": lambda row: now_utc_str(),
    "updated_at": lambda row: now_utc_str(),
}


# ---------------------------------------------------------------------------
# Value Cleaning & Helper Functions
# ---------------------------------------------------------------------------

NULL_STRINGS = {"", "n/a", "na", "null", "none", "nan", "undefined", "nil"}

def clean_value(val: Any) -> Any:
    """Cleans a raw CSV string value, converting null-like representations to Python None."""
    if val is None:
        return None
    if isinstance(val, str):
        stripped = val.strip()
        if stripped.lower() in NULL_STRINGS:
            return None
        return stripped
    return val

def resolve_mapped_value(rule: Any, row: Dict[str, str], csv_headers: Set[str]) -> Any:
    """
    Resolves the value for a database column based on the mapping rule:
    - None: returns None (SQL NULL)
    - ("const", val): returns val directly
    - callable: invokes rule(row)
    - str in csv_headers: returns row[rule]
    - str not in csv_headers: treats string as a constant literal (e.g. 'approved', 'google_review')
    """
    if rule is None:
        return None
    if isinstance(rule, (list, tuple)) and len(rule) == 2 and rule[0] == "const":
        return rule[1]
    if callable(rule):
        return rule(row)
    if isinstance(rule, str):
        if rule in csv_headers:
            return row.get(rule)
        # Explicit literal constants
        if rule.lower() in ("approved", "google_review", "google review", "pending", "google_maps"):
            if rule == "google review":
                return "google_review"  # Normalize to match database constraint
            return rule
        return None
    return None

def format_sql_literal(val: Any) -> str:
    """Formats a python value into safe SQL literal for SQL script generation."""
    if val is None:
        return "NULL"
    if isinstance(val, (int, float)):
        return str(val)
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    escaped = str(val).replace("'", "''")
    return f"'{escaped}'"

def format_table_name(table_name: str) -> str:
    """Formats table name, preserving schema names like 'public.houses'."""
    parts = table_name.split(".")
    return ".".join(f'"{p.strip()}"' for p in parts)

def detect_db_type(conn: Any) -> str:
    """Identifies if the connection is PostgreSQL or SQLite."""
    type_name = type(conn).__module__.lower()
    if "psycopg" in type_name or "postgres" in type_name:
        return "postgres"
    return "sqlite"

def get_existing_table_columns(conn: Any, table_name: str, db_type: str) -> Optional[Set[str]]:
    """Returns the set of existing columns for a table if it exists in the database."""
    try:
        cur = conn.cursor()
        if db_type == "postgres":
            parts = table_name.split(".")
            tbl = parts[-1].strip('"')
            schema = parts[0].strip('"') if len(parts) > 1 else "public"
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s AND table_schema = %s
            """, (tbl, schema))
            cols = [r[0] for r in cur.fetchall()]
            return set(cols) if cols else None
        elif db_type == "sqlite":
            clean_tbl = table_name.strip('"')
            cur.execute(f"PRAGMA table_info('{clean_tbl}')")
            cols = [r[1] for r in cur.fetchall()]
            return set(cols) if cols else None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Core Reusable Seeder Utility Function
# ---------------------------------------------------------------------------

def seed_csv_to_db(
    conn: Any,
    csv_path: Union[str, Path],
    table_name: str,
    column_mapping: Dict[str, Union[str, Callable[[Dict[str, str]], Any], Tuple[str, Any]]],
    mode: str = "upsert",
    conflict_columns: Optional[List[str]] = None,
    update_columns: Optional[List[str]] = None,
    batch_size: int = 50,
    delay: float = 0.1,
    clean_values: bool = True,
    db_type: Optional[str] = None,
    auto_create_table: bool = True,
    dry_run: bool = False
) -> int:
    """
    Directly binds CSV headers to database columns and seeds records slowly/gently into the database.

    :param conn: Active psycopg2 or sqlite3 connection.
    :param csv_path: Path to the source CSV file.
    :param table_name: Destination database table name.
    :param column_mapping: Dictionary mapping `db_column_name` -> `csv_header_name`.
    :param mode: 'upsert' (insert or update on conflict), 'insert' (plain insert), or 'update'.
    :param conflict_columns: List of columns defining unique constraint (e.g. ['id'] or ['house_id']).
    :param update_columns: Specific columns to update on conflict (defaults to all non-conflict columns).
    :param batch_size: Number of records to insert per batch (keep small for low DB load).
    :param delay: Seconds to sleep between batches to keep DB writing slow and controlled.
    :param clean_values: If True, converts empty strings, 'N/A', etc. to None/NULL.
    :param db_type: 'postgres' or 'sqlite' (auto-detected if None).
    :param auto_create_table: If True and table doesn't exist, creates table in SQLite.
    :param dry_run: If True, reads and validates mappings without executing DB queries.
    :return: Total number of rows processed.
    """
    global stop_requested

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV file not found: {csv_file}")

    if not dry_run and conn is None:
        raise ValueError("A valid database connection must be provided when dry_run=False.")

    if not dry_run:
        db_type = db_type or detect_db_type(conn)
    else:
        db_type = db_type or "postgres"

    # Count total rows for progress tracking
    with open(csv_file, "r", encoding="utf-8") as f:
        total_csv_rows = sum(1 for _ in f) - 1

    logger.info(f"==> Starting seed for table '{table_name}' from '{csv_file.name}'")
    logger.info(f"    Mode: {mode.upper()} | DB: {db_type.upper()} | Batch Size: {batch_size} | Throttle Delay: {delay}s")

    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        csv_headers = set(reader.fieldnames or [])

        # Check existing table columns in the database if available
        existing_cols = None if dry_run else get_existing_table_columns(conn, table_name, db_type)
        if existing_cols:
            logger.info(f"    Detected existing table schema ({len(existing_cols)} columns in DB).")

        # Determine active mapped columns:
        active_db_cols = []
        for db_col, mapping in column_mapping.items():
            if existing_cols is not None and db_col not in existing_cols:
                logger.debug(f"Column '{db_col}' not in table '{table_name}' schema. Skipping.")
                continue

            if mapping is None:
                active_db_cols.append(db_col)
            elif isinstance(mapping, (list, tuple)) and mapping[0] == "const":
                active_db_cols.append(db_col)
            elif callable(mapping):
                active_db_cols.append(db_col)
            elif isinstance(mapping, str):
                if mapping in csv_headers or mapping.lower() in ("approved", "google_review", "google review", "pending", "google_maps"):
                    active_db_cols.append(db_col)
                else:
                    logger.debug(f"CSV header '{mapping}' not found in CSV. Skipping '{db_col}'.")

        if not active_db_cols:
            raise ValueError(f"No mapped columns matched any headers in {csv_file.name} or target table!")

        logger.info(f"    Active mapped columns ({len(active_db_cols)}): {active_db_cols}")

        conflict_cols = conflict_columns or [active_db_cols[0]]
        if update_columns is None:
            upd_cols = [c for c in active_db_cols if c not in conflict_cols]
        else:
            upd_cols = [c for c in update_columns if c in active_db_cols]

        # Build SQL statement based on DB engine and mode
        placeholder = "%s" if db_type == "postgres" else "?"
        tbl_formatted = format_table_name(table_name)
        cols_str = ", ".join(f'"{c}"' for c in active_db_cols)
        vals_str = ", ".join([placeholder] * len(active_db_cols))

        if mode == "insert":
            if db_type == "sqlite":
                sql = f'INSERT OR IGNORE INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str})'
            else:
                sql = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT DO NOTHING'

        elif mode == "upsert":
            conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)
            if upd_cols:
                if db_type == "postgres":
                    updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in upd_cols)
                    sql = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_str}) DO UPDATE SET {updates}'
                else:
                    updates = ", ".join(f'"{c}" = excluded."{c}"' for c in upd_cols)
                    sql = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_str}) DO UPDATE SET {updates}'
            else:
                if db_type == "postgres":
                    sql = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_str}) DO NOTHING'
                else:
                    sql = f'INSERT OR IGNORE INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str})'

        elif mode == "update":
            set_str = ", ".join(f'"{c}" = {placeholder}' for c in upd_cols)
            where_str = " AND ".join(f'"{c}" = {placeholder}' for c in conflict_cols)
            sql = f'UPDATE {tbl_formatted} SET {set_str} WHERE {where_str}'
        else:
            raise ValueError(f"Unsupported mode '{mode}'. Choose 'upsert', 'insert', or 'update'.")

        if dry_run:
            logger.info(f"    [DRY RUN] Generated SQL Query:\n    {sql}")

        # Auto-create table if needed in SQLite when table doesn't already exist
        cur = conn.cursor() if not dry_run else None
        if not dry_run and auto_create_table and db_type == "sqlite" and existing_cols is None:
            col_defs = []
            for col in active_db_cols:
                if col in conflict_cols:
                    col_defs.append(f'"{col}" TEXT PRIMARY KEY')
                else:
                    col_defs.append(f'"{col}" TEXT')
            create_sql = f'CREATE TABLE IF NOT EXISTS {tbl_formatted} ({", ".join(col_defs)});'
            cur.execute(create_sql)
            conn.commit()

        # Check for psycopg2.extras execute_batch
        use_execute_batch = False
        if not dry_run and db_type == "postgres":
            try:
                import psycopg2.extras
                use_execute_batch = True
            except ImportError:
                use_execute_batch = False

        # Stream and process rows in batches with gentle throttling
        total_rows = 0
        skipped_unmapped = 0
        batch = []
        start_time = time.time()

        for row in reader:
            if stop_requested:
                logger.info(f"[!] Seeding interrupted by user. {total_rows} rows saved so far.")
                break

            # Skip unmapped properties without a valid business_id (prevents NOT NULL violation)
            if "business_id" in active_db_cols:
                b_val = resolve_mapped_value(column_mapping["business_id"], row, csv_headers)
                if clean_value(b_val) is None:
                    skipped_unmapped += 1
                    continue

            record = []
            if mode == "update":
                for c in upd_cols:
                    rule = column_mapping[c]
                    raw_val = resolve_mapped_value(rule, row, csv_headers)
                    if c == "comment":
                        final_val = "" if (raw_val is None or str(raw_val).strip() in NULL_STRINGS) else str(raw_val)
                    else:
                        final_val = clean_value(raw_val) if clean_values else raw_val
                    record.append(final_val)
                for c in conflict_cols:
                    rule = column_mapping[c]
                    raw_val = resolve_mapped_value(rule, row, csv_headers)
                    record.append(clean_value(raw_val) if clean_values else raw_val)
            else:
                for c in active_db_cols:
                    rule = column_mapping[c]
                    raw_val = resolve_mapped_value(rule, row, csv_headers)
                    if c == "comment":
                        final_val = "" if (raw_val is None or str(raw_val).strip() in NULL_STRINGS) else str(raw_val)
                    else:
                        final_val = clean_value(raw_val) if clean_values else raw_val
                    record.append(final_val)

            batch.append(tuple(record))
            total_rows += 1

            if len(batch) >= batch_size:
                if not dry_run:
                    if use_execute_batch:
                        psycopg2.extras.execute_batch(cur, sql, batch, page_size=len(batch))
                    else:
                        cur.executemany(sql, batch)
                    conn.commit()

                pct = (total_rows / total_csv_rows * 100) if total_csv_rows > 0 else 0
                logger.info(f"    [{total_rows:>5}/{total_csv_rows}] ({pct:>5.1f}%) - Written batch of {len(batch)} to {table_name}")
                batch = []

                # Gentle sleep between batches to keep database load low
                if delay > 0 and not dry_run:
                    time.sleep(delay)

        # Flush remaining rows
        if batch and not stop_requested:
            if not dry_run:
                if use_execute_batch:
                    psycopg2.extras.execute_batch(cur, sql, batch, page_size=len(batch))
                else:
                    cur.executemany(sql, batch)
                conn.commit()

            pct = (total_rows / total_csv_rows * 100) if total_csv_rows > 0 else 100
            logger.info(f"    [{total_rows:>5}/{total_csv_rows}] ({pct:>5.1f}%) - Written final batch of {len(batch)} to {table_name}")

        duration = time.time() - start_time
        if skipped_unmapped:
            logger.info(f"    (Safely skipped {skipped_unmapped} unmapped properties with no Google Maps business ID)")
        logger.info(f"✓ Completed seeding {total_rows} rows into '{table_name}' in {duration:.2f}s.")
        return total_rows


# ---------------------------------------------------------------------------
# SQL Script Exporter (Optional File Generation)
# ---------------------------------------------------------------------------

def export_mapped_csv_to_sql(
    csv_path: Union[str, Path],
    output_sql_path: Union[str, Path],
    table_name: str,
    column_mapping: Dict[str, Any],
    conflict_columns: Optional[List[str]] = None,
    mode: str = "upsert"
) -> int:
    """Exports CSV data into a ready-to-run PostgreSQL .sql script using the column bindings."""
    csv_file = Path(csv_path)
    sql_file = Path(output_sql_path)
    
    with open(csv_file, "r", encoding="utf-8") as f, open(sql_file, "w", encoding="utf-8") as out:
        reader = csv.DictReader(f)
        csv_headers = set(reader.fieldnames or [])
        
        active_db_cols = []
        for db_col, mapping in column_mapping.items():
            if mapping is None:
                active_db_cols.append(db_col)
            elif isinstance(mapping, (list, tuple)) and mapping[0] == "const":
                active_db_cols.append(db_col)
            elif callable(mapping):
                active_db_cols.append(db_col)
            elif isinstance(mapping, str):
                if mapping in csv_headers or mapping.lower() in ("approved", "google_review", "google review", "pending", "google_maps"):
                    active_db_cols.append(db_col)
                    
        conflict_cols = conflict_columns or [active_db_cols[0]]
        upd_cols = [c for c in active_db_cols if c not in conflict_cols]

        out.write(f"-- Generated SQL Seed Script for {table_name}\n")
        out.write(f"-- Source: {csv_file.name}\n\n")

        tbl_formatted = format_table_name(table_name)
        cols_str = ", ".join(f'"{c}"' for c in active_db_cols)
        conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)
        updates = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in upd_cols)

        count = 0
        for row in reader:
            vals = []
            for c in active_db_cols:
                rule = column_mapping[c]
                val = resolve_mapped_value(rule, row, csv_headers)
                vals.append(format_sql_literal(clean_value(val)))
            vals_str = ", ".join(vals)

            if mode == "upsert" and upd_cols:
                stmt = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT ({conflict_str}) DO UPDATE SET {updates};\n'
            elif mode == "insert":
                stmt = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str}) ON CONFLICT DO NOTHING;\n'
            else:
                stmt = f'INSERT INTO {tbl_formatted} ({cols_str}) VALUES ({vals_str});\n'

            out.write(stmt)
            count += 1

    logger.info(f"✓ Exported {count} SQL statements to: {sql_file}")
    return count


# ---------------------------------------------------------------------------
# High-Level Conveniences for Seeding Reviews & Business IDs
# ---------------------------------------------------------------------------

def seed_reviews(
    conn: Any = None,
    csv_path: Union[str, Path] = DEFAULT_REVIEWS_CSV,
    table_name: str = "new_house_reviews",
    column_mapping: Optional[Dict[str, str]] = None,
    mode: str = "upsert",
    conflict_columns: Optional[List[str]] = None,
    batch_size: int = 50,
    delay: float = 0.1,
    dry_run: bool = False,
    db_type: Optional[str] = None
) -> int:
    """Seeds scraped reviews from CSV into the database reviews table slowly and gently."""
    mapping = column_mapping or new_house_reviews
    conflicts = conflict_columns or ["id"]
    return seed_csv_to_db(
        conn=conn,
        csv_path=csv_path,
        table_name=table_name,
        column_mapping=mapping,
        mode=mode,
        conflict_columns=conflicts,
        batch_size=batch_size,
        delay=delay,
        dry_run=dry_run,
        db_type=db_type
    )

def seed_business_ids(
    conn: Any = None,
    csv_path: Union[str, Path] = DEFAULT_BUSINESS_IDS_CSV,
    table_name: str = "house_business_mappings",
    column_mapping: Optional[Dict[str, str]] = None,
    mode: str = "upsert",
    conflict_columns: Optional[List[str]] = None,
    batch_size: int = 50,
    delay: float = 0.1,
    dry_run: bool = False,
    db_type: Optional[str] = None
) -> int:
    """Seeds Google Maps business IDs into the database slowly and gently."""
    mapping = column_mapping or house_business_mappings
    conflicts = conflict_columns or ["house_id"]
    return seed_csv_to_db(
        conn=conn,
        csv_path=csv_path,
        table_name=table_name,
        column_mapping=mapping,
        mode=mode,
        conflict_columns=conflicts,
        batch_size=batch_size,
        delay=delay,
        dry_run=dry_run,
        db_type=db_type
    )


# ---------------------------------------------------------------------------
# Database Connection Resolver
# ---------------------------------------------------------------------------

def get_database_connection(
    db_type: Optional[str] = None,
    db_url: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    dbname: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    sqlite_path: Optional[str] = None
) -> Tuple[Any, str]:
    """Establishes database connection with automatic fallback to local SQLite."""
    env_db_type = os.getenv("DB_TYPE", "postgres").lower()
    target_type = (db_type or env_db_type).lower()

    if target_type == "sqlite":
        import sqlite3
        path = sqlite_path or os.getenv("SQLITE_PATH", DEFAULT_SQLITE_DB)
        logger.info(f"Connecting to SQLite database: {path}")
        conn = sqlite3.connect(path)
        return conn, "sqlite"

    try:
        import psycopg2
        url = db_url or os.getenv("DATABASE_URL")
        if url:
            logger.info("Connecting to PostgreSQL via DATABASE_URL...")
            conn = psycopg2.connect(url)
            return conn, "postgres"

        h = host or os.getenv("DB_HOST", "localhost")
        p = port or int(os.getenv("DB_PORT", "5432"))
        d = dbname or os.getenv("DB_NAME", "postgres")
        u = user or os.getenv("DB_USER", "postgres")
        pwd = password or os.getenv("DB_PASSWORD", "postgres")

        logger.info(f"Connecting to PostgreSQL at {h}:{p}/{d}...")
        conn = psycopg2.connect(
            host=h,
            port=p,
            dbname=d,
            user=u,
            password=pwd,
            connect_timeout=8
        )
        return conn, "postgres"
    except Exception as e:
        logger.warning(f"Could not connect to PostgreSQL ({e}).")
        import sqlite3
        path = sqlite_path or os.getenv("SQLITE_PATH", DEFAULT_SQLITE_DB)
        logger.info(f"Falling back to local SQLite database: {path}")
        conn = sqlite3.connect(path)
        return conn, "sqlite"


# ---------------------------------------------------------------------------
# CLI Command Runner
# ---------------------------------------------------------------------------

def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="Seed property reviews and business IDs to PostgreSQL or SQLite database."
    )
    parser.add_argument("--all", action="store_true", help="Seed both business IDs and reviews.")
    parser.add_argument("--reviews", action="store_true", help="Seed reviews from CSV.")
    parser.add_argument("--business-ids", action="store_true", help="Seed business IDs from CSV.")
    parser.add_argument("--reviews-csv", default=DEFAULT_REVIEWS_CSV, help="Custom reviews CSV path.")
    parser.add_argument("--business-ids-csv", default=DEFAULT_BUSINESS_IDS_CSV, help="Custom business IDs CSV path.")
    parser.add_argument("--reviews-table", default=os.getenv("REVIEWS_TABLE", "new_house_reviews"), help="Reviews table name (default: new_house_reviews).")
    parser.add_argument("--business-table", "--properties-table", dest="business_table", default=os.getenv("BUSINESS_TABLE", "house_business_mappings"), help="Business mappings table name (default: house_business_mappings).")
    parser.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "50")), help="Batch size for writing (default: 50).")
    parser.add_argument("--delay", type=float, default=float(os.getenv("BATCH_DELAY", "0.1")), help="Seconds to sleep between batches (default: 0.1s).")
    parser.add_argument("--mode", choices=["upsert", "insert", "update"], default="upsert", help="Insertion mode.")
    parser.add_argument("--db-type", choices=["postgres", "sqlite"], default=os.getenv("DB_TYPE"), help="Database engine.")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"), help="PostgreSQL connection string.")
    parser.add_argument("--sqlite-path", default=os.getenv("SQLITE_PATH", DEFAULT_SQLITE_DB), help="SQLite database path.")
    parser.add_argument("--dry-run", action="store_true", help="Validate mappings and SQL without modifying database.")
    parser.add_argument("--export-sql", action="store_true", help="Export as SQL seed script (.sql file).")
    return parser.parse_args()

def main():
    args = parse_cli_args()

    do_all = args.all or (not args.reviews and not args.business_ids)
    do_reviews = do_all or args.reviews
    do_bids = do_all or args.business_ids

    if args.export_sql:
        if do_bids:
            out_path = CURRENT_DIR / "seed_house_business_mappings.sql"
            export_mapped_csv_to_sql(
                args.business_ids_csv,
                out_path,
                table_name=args.business_table,
                column_mapping=house_business_mappings,
                conflict_columns=["house_id"],
                mode=args.mode
            )
        if do_reviews:
            out_path = CURRENT_DIR / "seed_house_reviews.sql"
            export_mapped_csv_to_sql(
                args.reviews_csv,
                out_path,
                table_name=args.reviews_table,
                column_mapping=new_house_reviews,
                conflict_columns=["id"],
                mode=args.mode
            )
        return

    conn = None
    resolved_db_type = args.db_type or "postgres"

    if not args.dry_run:
        conn, resolved_db_type = get_database_connection(
            db_type=args.db_type,
            db_url=args.db_url,
            sqlite_path=args.sqlite_path
        )

    try:
        if do_bids and not stop_requested:
            seed_business_ids(
                conn=conn,
                csv_path=args.business_ids_csv,
                table_name=args.business_table,
                mode=args.mode,
                batch_size=args.batch_size,
                delay=args.delay,
                dry_run=args.dry_run,
                db_type=resolved_db_type
            )

        if do_reviews and not stop_requested:
            seed_reviews(
                conn=conn,
                csv_path=args.reviews_csv,
                table_name=args.reviews_table,
                mode=args.mode,
                batch_size=args.batch_size,
                delay=args.delay,
                dry_run=args.dry_run,
                db_type=resolved_db_type
            )

        if not stop_requested:
            logger.info("🎉 Database seeding finished successfully!")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()
