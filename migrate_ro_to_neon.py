import sqlite3
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd

# === 1) PATH TO YOUR LOCAL SQLITE DB ===
SQLITE_DB = r"C:\Users\acer\OneDrive\Nileps\NFM_RO\ro_uaq.db"

# === 2) YOUR NEON CONNECTION URL ===
NEON_URL = (
    "postgresql://neondb_owner:"
    "npg_C4ghxK1yUcfw@"
    "ep-billowing-fog-agxbr2fc-pooler.c-2.eu-central-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

# -------------------------------------------------
#  Create tables in Neon (same schema as new RO app)
# -------------------------------------------------
def create_tables_pg(conn):
    cur = conn.cursor()

    # readings
    cur.execute("""
        CREATE TABLE IF NOT EXISTS readings (
            id SERIAL PRIMARY KEY,
            d DATE,
            tds DOUBLE PRECISION,
            ph DOUBLE PRECISION,
            conductivity DOUBLE PRECISION,
            flow_m3 DOUBLE PRECISION,
            production DOUBLE PRECISION,
            maintenance TEXT,
            notes TEXT
        );
    """)

    # cartridge
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cartridge (
            id SERIAL PRIMARY KEY,
            d DATE,
            dp DOUBLE PRECISION,
            remarks TEXT,
            is_change INTEGER DEFAULT 0,
            change_cost DOUBLE PRECISION DEFAULT 0
        );
    """)

    # chemicals
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chemicals (
            name TEXT PRIMARY KEY,
            qty DOUBLE PRECISION,
            unit_cost DOUBLE PRECISION DEFAULT 0
        );
    """)

    # chemical movements
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chemical_movements (
            id SERIAL PRIMARY KEY,
            d DATE,
            name TEXT,
            movement_type TEXT,
            qty DOUBLE PRECISION,
            remarks TEXT
        );
    """)

    conn.commit()
    cur.close()


# -------------------------------------------------
#  Helpers to load from SQLite
# -------------------------------------------------
def load_sqlite_table(table_name):
    conn = sqlite3.connect(SQLITE_DB)
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    conn.close()
    print(f"[SQLite] Loaded {len(df)} rows from {table_name}")
    return df


# -------------------------------------------------
#  Migrate each table
# -------------------------------------------------
def migrate_readings(pg_conn):
    df = load_sqlite_table("readings")
    if df.empty:
        print("[readings] No data to migrate.")
        return

    # Some old DBs may not have all columns – fill if missing
    for col in ["d", "tds", "ph", "conductivity", "flow_m3", "production", "maintenance", "notes"]:
        if col not in df.columns:
            df[col] = None

    rows = list(
        zip(
            df["d"],
            df["tds"],
            df["ph"],
            df["conductivity"],
            df["flow_m3"],
            df["production"],
            df["maintenance"],
            df["notes"],
        )
    )

    cur = pg_conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO readings
            (d, tds, ph, conductivity, flow_m3, production, maintenance, notes)
        VALUES %s
        """,
        rows,
    )
    pg_conn.commit()
    cur.close()
    print(f"[readings] Migrated {len(rows)} rows to Neon.")


def migrate_cartridge(pg_conn):
    df = load_sqlite_table("cartridge")
    if df.empty:
        print("[cartridge] No data to migrate.")
        return

    for col in ["d", "dp", "remarks", "is_change", "change_cost"]:
        if col not in df.columns:
            df[col] = 0 if col in ["dp", "is_change", "change_cost"] else ""

    rows = list(
        zip(
            df["d"],
            df["dp"],
            df["remarks"],
            df["is_change"],
            df["change_cost"],
        )
    )

    cur = pg_conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO cartridge
            (d, dp, remarks, is_change, change_cost)
        VALUES %s
        """,
        rows,
    )
    pg_conn.commit()
    cur.close()
    print(f"[cartridge] Migrated {len(rows)} rows to Neon.")


def migrate_chemicals(pg_conn):
    df = load_sqlite_table("chemicals")
    if df.empty:
        print("[chemicals] No data to migrate.")
        return

    for col in ["name", "qty", "unit_cost"]:
        if col not in df.columns:
            if col == "name":
                df[col] = ""
            else:
                df[col] = 0.0

    rows = list(
        zip(
            df["name"],
            df["qty"],
            df["unit_cost"],
        )
    )

    cur = pg_conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO chemicals
            (name, qty, unit_cost)
        VALUES %s
        ON CONFLICT (name) DO UPDATE
        SET qty = EXCLUDED.qty,
            unit_cost = EXCLUDED.unit_cost
        """,
        rows,
    )
    pg_conn.commit()
    cur.close()
    print(f"[chemicals] Migrated {len(rows)} rows to Neon.")


def migrate_chemical_movements(pg_conn):
    df = load_sqlite_table("chemical_movements")
    if df.empty:
        print("[chemical_movements] No data to migrate.")
        return

    for col in ["d", "name", "movement_type", "qty", "remarks"]:
        if col not in df.columns:
            if col in ["qty"]:
                df[col] = 0.0
            else:
                df[col] = ""

    rows = list(
        zip(
            df["d"],
            df["name"],
            df["movement_type"],
            df["qty"],
            df["remarks"],
        )
    )

    cur = pg_conn.cursor()
    execute_values(
        cur,
        """
        INSERT INTO chemical_movements
            (d, name, movement_type, qty, remarks)
        VALUES %s
        """,
        rows,
    )
    pg_conn.commit()
    cur.close()
    print(f"[chemical_movements] Migrated {len(rows)} rows to Neon.")


def main():
    print("[1] Connecting to Neon...")
    pg_conn = psycopg2.connect(NEON_URL)

    print("[2] Creating tables if not exist...")
    create_tables_pg(pg_conn)

    print("[3] Migrating readings...")
    try:
        migrate_readings(pg_conn)
    except Exception as e:
        print(f"[ERROR] readings: {e}")

    print("[4] Migrating cartridge...")
    try:
        migrate_cartridge(pg_conn)
    except Exception as e:
        print(f"[ERROR] cartridge: {e}")

    print("[5] Migrating chemicals...")
    try:
        migrate_chemicals(pg_conn)
    except Exception as e:
        print(f"[ERROR] chemicals: {e}")

    print("[6] Migrating chemical_movements...")
    try:
        migrate_chemical_movements(pg_conn)
    except Exception as e:
        print(f"[ERROR] chemical_movements: {e}")

    pg_conn.close()
    print("✅ Migration finished.")


if __name__ == "__main__":
    main()
