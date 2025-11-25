import streamlit as st
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import io

st.set_page_config(page_title="RO Um Qasr – Neon", layout="wide")

# -----------------------------
# CONFIG / CONSTANTS
# -----------------------------
DB_URL = "postgresql://neondb_owner:npg_C4ghxK1yUcfw@ep-billowing-fog-agxbr2fc-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
PLANT_CAPACITY = 10  # m3/day

MIN_STOCK = {
    "Sodium Hypochlorite (Chlorine)": 50,
    "Hydrochloric Acid (HCL)": 30,
    "Antiscalant PC-391": 25,
}

PRIMARY_COLOR = "#059669"  # emerald
DANGER_COLOR = "#DC2626"
WARN_COLOR = "#F97316"

# -----------------------------
# DB CONNECTION HELPERS
# -----------------------------
def get_conn():
    return psycopg2.connect(DB_URL)

def init_db():
    conn = get_conn()
    conn.autocommit = True
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
    # seed chemicals if empty
    cur.execute("SELECT COUNT(*) FROM chemicals;")
    n = cur.fetchone()[0]
    if n == 0:
        cur.execute("""
            INSERT INTO chemicals (name, qty, unit_cost) VALUES
            ('Sodium Hypochlorite (Chlorine)', 0, 0),
            ('Hydrochloric Acid (HCL)', 0, 0),
            ('Antiscalant PC-391', 0, 0)
        """)
    cur.close()
    conn.close()

@st.cache_data(ttl=60)
def get_readings():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM readings ORDER BY d", conn)
    conn.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]
    return df

@st.cache_data(ttl=60)
def get_chemicals():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM chemicals ORDER BY name", conn)
    conn.close()
    return df

@st.cache_data(ttl=60)
def get_chemical_movements():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM chemical_movements ORDER BY d", conn)
    conn.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]
    return df

# -----------------------------
# UI HELPERS
# -----------------------------
def kpi_card(label, value, help_text=None):
    st.markdown(
        f"""<div style='border-radius:12px;border:1px solid #e5e7eb;padding:12px 16px;background:white'>
        <div style='font-size:12px;color:#6b7280'>{label}</div>
        <div style='font-size:22px;font-weight:600;color:#111827'>{value}</div>
        {f"<div style='font-size:11px;color:#9ca3af'>{help_text}</div>" if help_text else ""}
        </div>""",
        unsafe_allow_html=True,
    )

def section_title(title: str):
    st.markdown(f"### {title}")

# -----------------------------
# DASHBOARD
# -----------------------------
def page_dashboard():
    readings = get_readings()
    chems = get_chemicals()
    chem_mov = get_chemical_movements()

    st.title("🌊 RO System Dashboard – Um Qasr")
    st.caption("Connected to Neon PostgreSQL • Capacity 10 m³/day")

    # --- KPI Row 1: Production & Quality ---
    col1, col2, col3, col4 = st.columns(4)

    today = datetime.now().date()
    last_30 = today - timedelta(days=30)

    if len(readings) > 0:
        df30 = readings[readings["d"].dt.date >= last_30]
    else:
        df30 = pd.DataFrame(columns=readings.columns if len(readings)>0 else [])

    prod_today = 0.0
    if len(readings) > 0:
        today_rows = readings[readings["d"].dt.date == today]
        if len(today_rows) > 0:
            prod_today = float(today_rows["production"].sum())

    prod_30 = float(df30["production"].sum()) if len(df30) > 0 else 0.0
    avg_tds = readings["tds"].mean() if "tds" in readings else 0
    avg_ph = readings["ph"].mean() if "ph" in readings else 0

    with col1:
        kpi_card("Today's Production", f"{prod_today:.1f} m³", f"Capacity {PLANT_CAPACITY} m³/day")
    with col2:
        kpi_card("30-day Production", f"{prod_30:.1f} m³")
    with col3:
        kpi_card("Avg TDS (all data)", f"{avg_tds:.0f} ppm")
    with col4:
        kpi_card("Avg pH (all data)", f"{avg_ph:.2f}")

    # --- Chemical cost per m3 over last 30d ---
    chem_cost_per_m3_30 = 0.0
    if len(chem_mov) > 0 and prod_30 > 0:
        mov30 = chem_mov[chem_mov["d"].dt.date >= last_30]
        out30 = mov30[mov30["movement_type"] == "OUT"]
        if len(out30) > 0:
            chems_df = get_chemicals()
            cost_map = {row["name"]: float(row["unit_cost"]) for _, row in chems_df.iterrows()} if len(chems_df) > 0 else {}
            usage = out30.groupby("name")["qty"].sum().reset_index()
            total_cost_30 = 0.0
            for _, r in usage.iterrows():
                nm = r["name"]
                qty = float(r["qty"])
                unit_cost = cost_map.get(nm, 0.0)
                total_cost_30 += qty * unit_cost
            chem_cost_per_m3_30 = total_cost_30 / prod_30

    st.markdown(" ")
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        kpi_card("Chem Cost / m³ (30d)", f"{chem_cost_per_m3_30:.4f}")
    with col6:
        kpi_card("Readings Count", f"{len(readings)}", "Total days with any reading")
    with col7:
        kpi_card("Chemicals Tracked", f"{len(chems)}")
    with col8:
        kpi_card("Movements Logged", f"{len(chem_mov)}")

    st.markdown("---")

    # --- Chemical Stock & Alerts ---
    section_title("⚗️ Chemical Stock & Alerts")

    if len(chems) == 0:
        st.info("No chemicals found in database.")
    else:
        df_disp = chems.copy()
        df_disp["stock_value"] = df_disp["qty"] * df_disp["unit_cost"]
        st.dataframe(df_disp, use_container_width=True)

        for _, row in chems.iterrows():
            nm = row["name"]
            qty = float(row["qty"])
            min_stock = MIN_STOCK.get(nm, 30)

            st.markdown(f"**{nm}: {qty:.1f} kg (Min {min_stock} kg)**")
            st.progress(min(qty / max(min_stock * 2, 1), 1.0))

            # Chlorine special rule
            if "Hypochlorite" in nm or "Chlorine" in nm:
                if qty < 50:
                    st.error(f"❗ URGENT: Chlorine stock {qty:.1f} kg < 50 kg")
                elif qty < 80:
                    st.warning(f"⚠ Chlorine stock {qty:.1f} kg – approaching low level.")
                continue

            if qty < min_stock:
                st.error(f"❗ {nm} below minimum stock ({qty:.1f} kg < {min_stock} kg).")
            elif qty < min_stock * 1.3:
                st.warning(f"⚠ {nm} approaching minimum stock.")

    st.markdown("---")

    # --- Daily Chemical Usage Chart (last 30d, OUT only) ---
    section_title("📊 Daily Chemical Usage – Last 30 Days (kg, OUT only)")
    if len(chem_mov) == 0:
        st.info("No chemical movements recorded yet.")
    else:
        mov30 = chem_mov[chem_mov["d"].dt.date >= last_30]
        out30 = mov30[mov30["movement_type"] == "OUT"]
        if len(out30) == 0:
            st.info("No OUT movements in the last 30 days.")
        else:
            df_daily = out30.copy()
            df_daily["qty"] = pd.to_numeric(df_daily["qty"], errors="coerce").fillna(0)
            daily_usage = df_daily.groupby("d")["qty"].sum().reset_index()

            fig, ax = plt.subplots(figsize=(8,3))
            ax.bar(daily_usage["d"], daily_usage["qty"], color=PRIMARY_COLOR)
            ax.set_ylabel("kg")
            ax.set_xlabel("Date")
            ax.set_title("Total Chemical Usage per Day (OUT)")
            ax.grid(alpha=0.3)
            plt.xticks(rotation=45)
            st.pyplot(fig)

# -----------------------------
# MONTHLY REPORT PAGE
# -----------------------------
def page_monthly_report():
    readings = get_readings()
    st.title("📅 Monthly Water Quality Report")

    if len(readings) == 0:
        st.info("No readings found in database.")
        return

    readings = readings.sort_values("d")
    readings["month"] = readings["d"].dt.to_period("M").astype(str)
    months = sorted(readings["month"].unique())
    month = st.selectbox("Select month", months, index=len(months)-1)

    df_m = readings[readings["month"] == month].copy()
    if len(df_m) == 0:
        st.info("No readings for this month.")
        return

    year, mon = map(int, month.split("-"))
    month_start = datetime(year, mon, 1)
    if mon == 12:
        next_month = datetime(year+1, 1, 1)
    else:
        next_month = datetime(year, mon+1, 1)
    month_end = next_month - timedelta(days=1)

    col1, col2, col3, col4 = st.columns(4)
    total_prod = float(df_m["production"].sum()) if "production" in df_m else 0.0
    avg_tds = df_m["tds"].mean() if "tds" in df_m else 0.0
    avg_ph = df_m["ph"].mean() if "ph" in df_m else 0.0
    days = (month_end - month_start).days + 1

    with col1:
        kpi_card("Total Production", f"{total_prod:.1f} m³", f"{month_start:%b %Y}")
    with col2:
        kpi_card("Avg TDS", f"{avg_tds:.0f} ppm")
    with col3:
        kpi_card("Avg pH", f"{avg_ph:.2f}")
    with col4:
        kpi_card("Days in Month", f"{days}")

    st.markdown("---")

    section_title("Daily Details (Existing Readings)")
    st.dataframe(
        df_m[
            ["d", "tds", "ph", "conductivity", "flow_m3", "production", "maintenance", "notes"]
        ],
        use_container_width=True,
    )

    section_title("Monthly Reading Matrix (1–31) – TDS / pH / Conductivity")

    all_days = pd.date_range(month_start, month_end, freq="D")
    base = pd.DataFrame({"d": all_days})

    if len(df_m) > 0:
        daily_simple = df_m[["d", "tds", "ph", "conductivity"]].copy()
    else:
        daily_simple = pd.DataFrame(columns=["d", "tds", "ph", "conductivity"])

    matrix = base.merge(daily_simple, on="d", how="left").sort_values("d")
    matrix.rename(
        columns={"d": "Date", "tds": "TDS (ppm)", "ph": "pH", "conductivity": "Conductivity (µS/cm)"},
        inplace=True,
    )
    st.dataframe(matrix, use_container_width=True)

    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        matrix.to_excel(writer, index=False, sheet_name="Readings_1_31")
    excel_data = output.getvalue()

    st.download_button(
        label="⬇ Download Monthly Reading Excel (1–31)",
        data=excel_data,
        file_name=f"RO_Readings_{month}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

# -----------------------------
# MAIN
# -----------------------------
def main():
    init_db()

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Dashboard", "Monthly Report"])

    if page == "Dashboard":
        page_dashboard()
    else:
        page_monthly_report()

if __name__ == "__main__":
    main()
