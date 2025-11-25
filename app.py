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

@st.cache_data(ttl=60)
def get_cartridge():
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM cartridge ORDER BY d", conn)
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

        edited_chem = st.data_editor(df_disp, num_rows="dynamic", key="chem_editor", use_container_width=True)
        if st.button("💾 Save Chemical Table Changes"):
            conn = get_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM chemicals;")
            for _, r in edited_chem.iterrows():
                cur.execute(
                    "INSERT INTO chemicals (name, qty, unit_cost) VALUES (%s, %s, %s)",
                    (r["name"], float(r.get("qty") or 0), float(r.get("unit_cost") or 0)),
                )
            conn.commit()
            cur.close()
            conn.close()
            st.success("Chemical stock table updated.")
            st.cache_data.clear()
            chems = get_chemicals()
            df_disp = chems.copy()
            df_disp["stock_value"] = df_disp["qty"] * df_disp["unit_cost"]

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
# CARTRIDGE FILTER PAGE
# -----------------------------
def page_cartridge():
    st.title("🧱 Cartridge Filter – Differential Pressure & Status")

    cart = get_cartridge()

    # Quick status from latest record
    last_dp = None
    last_change_date = None
    if len(cart) > 0:
        last_row = cart.sort_values("d").iloc[-1]
        last_dp = float(last_row.get("dp") or 0)
        if int(last_row.get("is_change") or 0) == 1:
            last_change_date = last_row["d"].date()

    col1, col2, col3 = st.columns(3)
    with col1:
        if last_dp is not None:
            kpi_card("Latest DP", f"{last_dp:.2f} bar")
        else:
            kpi_card("Latest DP", "N/A")
    with col2:
        if last_change_date:
            kpi_card("Last Filter Change", f"{last_change_date:%Y-%m-%d}")
        else:
            kpi_card("Last Filter Change", "No change recorded")
    with col3:
        if last_dp is not None:
            if last_dp >= 1.5:
                kpi_card("Status", "CHANGE NOW", "DP ≥ 1.5 bar – replace cartridge")
                st.error("Cartridge DP is high – change required.")
            elif last_dp >= 1.0:
                kpi_card("Status", "Monitor", "DP between 1.0 and 1.5 bar")
                st.warning("Cartridge DP elevated – monitor closely.")
            else:
                kpi_card("Status", "OK", "DP below 1.0 bar")
        else:
            kpi_card("Status", "Unknown")

    st.markdown("---")

    # Add new reading form
    st.subheader("Add New Cartridge DP Reading")
    with st.form("add_cart_reading"):
        c1, c2 = st.columns(2)
        with c1:
            d = st.date_input("Date", value=datetime.now().date())
            dp = st.number_input("Differential Pressure (bar)", min_value=0.0, max_value=10.0, value=0.0, step=0.1)
        with c2:
            is_change = st.checkbox("Filter changed in this reading?")
            change_cost = st.number_input("Change Cost", min_value=0.0, value=0.0, step=1.0)
        remarks = st.text_input("Remarks", "")
        submitted = st.form_submit_button("Save Reading")

    if submitted:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cartridge (d, dp, remarks, is_change, change_cost) VALUES (%s, %s, %s, %s, %s)",
            (d, dp, remarks, 1 if is_change else 0, change_cost),
        )
        conn.commit()
        cur.close()
        conn.close()
        st.success("Cartridge reading saved.")
        st.cache_data.clear()

    st.markdown("---")

    # Chart of DP over time
    st.subheader("DP Trend")
    cart = get_cartridge()
    if len(cart) == 0:
        st.info("No cartridge records yet.")
    else:
        fig, ax = plt.subplots(figsize=(8, 3))
        ax.plot(cart["d"], cart["dp"], marker="o", markersize=3)
        ax.set_xlabel("Date")
        ax.set_ylabel("DP (bar)")
        ax.set_title("Cartridge Differential Pressure Over Time")
        ax.grid(alpha=0.3)
        plt.xticks(rotation=45)
        st.pyplot(fig)

        st.subheader("Edit Cartridge Records")
        editable = cart.copy()
        editable["d"] = editable["d"].dt.date
        edited = st.data_editor(editable, num_rows="dynamic", key="cart_editor", use_container_width=True)
        if st.button("💾 Save Cartridge Table Changes"):
            conn = get_conn()
            cur = conn.cursor()
            # update by id; assume id exists
            for _, row in edited.iterrows():
                cur.execute(
                    "UPDATE cartridge SET d=%s, dp=%s, remarks=%s, is_change=%s, change_cost=%s WHERE id=%s",
                    (
                        row["d"],
                        float(row.get("dp") or 0),
                        row.get("remarks") or "",
                        int(row.get("is_change") or 0),
                        float(row.get("change_cost") or 0),
                        int(row["id"]),
                    ),
                )
            conn.commit()
            cur.close()
            conn.close()
            st.success("Cartridge table updated.")
            st.cache_data.clear()
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

    # compute month range
    year, mon = map(int, month.split("-"))
    month_start = datetime(year, mon, 1)
    if mon == 12:
        next_month = datetime(year+1, 1, 1)
    else:
        next_month = datetime(year, mon+1, 1)
    month_end = next_month - timedelta(days=1)

    # date range filter inside month
    st.write("Filter readings within the selected month:")
    start_date, end_date = st.date_input(
        "Date range",
        value=(month_start.date(), month_end.date()),
        min_value=month_start.date(),
        max_value=month_end.date(),
    )

    df_m = readings[readings["month"] == month].copy()
    if len(df_m) == 0:
        st.info("No readings for this month.")
        return

    # apply range filter
    df_m = df_m[(df_m["d"].dt.date >= start_date) & (df_m["d"].dt.date <= end_date)]

    if len(df_m) == 0:
        st.info("No readings in this date range.")
        return

    col1, col2, col3, col4 = st.columns(4)
    total_prod = float(df_m["production"].sum()) if "production" in df_m else 0.0
    avg_tds = df_m["tds"].mean() if "tds" in df_m else 0.0
    avg_ph = df_m["ph"].mean() if "ph" in df_m else 0.0
    days_span = (end_date - start_date).days + 1

    with col1:
        kpi_card("Total Production", f"{total_prod:.1f} m³", f"{month_start:%b %Y}")
    with col2:
        kpi_card("Avg TDS", f"{avg_tds:.0f} ppm")
    with col3:
        kpi_card("Avg pH", f"{avg_ph:.2f}")
    with col4:
        kpi_card("Days in Range", f"{days_span}")

    st.markdown("---")

    # Trend chart with small markers
    section_title("Quality Trend (TDS & pH)")
    fig, ax1 = plt.subplots(figsize=(8,3))
    ax1.plot(df_m["d"], df_m["tds"], marker="o", markersize=3, label="TDS (ppm)")
    ax1.set_ylabel("TDS (ppm)")
    ax1.grid(alpha=0.3)

    ax2 = ax1.twinx()
    ax2.plot(df_m["d"], df_m["ph"], marker="s", markersize=3, linestyle="--", label="pH", color="#3b82f6")
    ax2.set_ylabel("pH")

    fig.autofmt_xdate()
    ax1.set_xlabel("Date")
    ax1.set_title("TDS & pH vs Date (Small Markers)")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1+lines2, labels1+labels2, loc="upper right")
    st.pyplot(fig)

    # Daily details
    section_title("Daily Details (Existing Readings)")
    st.dataframe(
        df_m[
            ["d", "tds", "ph", "conductivity", "flow_m3", "production", "maintenance", "notes"]
        ],
        use_container_width=True,
    )

    # Maintenance summary
    section_title("Maintenance & Notes Summary")
    maint = df_m[
        df_m["maintenance"].astype(str).str.strip() != ""
    ][["d", "maintenance", "notes"]].copy()
    if len(maint) == 0:
        st.info("No maintenance records in this period.")
    else:
        st.dataframe(maint, use_container_width=True)

    # Monthly reading matrix (full month 1–31)
    section_title("Monthly Reading Matrix (1–31) – TDS / pH / Conductivity (Full Month)")

    all_days = pd.date_range(month_start, month_end, freq="D")
    base = pd.DataFrame({{"d": all_days}})

    full_month = readings[readings["month"] == month].copy()
    if len(full_month) > 0:
        daily_simple = full_month[["d", "tds", "ph", "conductivity"]].copy()
    else:
        daily_simple = pd.DataFrame(columns=["d", "tds", "ph", "conductivity"])

    matrix = base.merge(daily_simple, on="d", how="left").sort_values("d")
    matrix.rename(
        columns={{"d": "Date", "tds": "TDS (ppm)", "ph": "pH", "conductivity": "Conductivity (µS/cm)"}},
        inplace=True,
    )
    st.dataframe(matrix, use_container_width=True)

    # Excel export with safe fallback if xlsxwriter missing
    output = io.BytesIO()
    excel_data = None
    try:
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            matrix.to_excel(writer, index=False, sheet_name="Readings_1_31")
        excel_data = output.getvalue()
    except ImportError:
        st.error("Excel export requires the 'xlsxwriter' package. Please add it to requirements.txt.")

    if excel_data:
        st.download_button(
            label="⬇ Download Monthly Reading Excel (1–31)",
            data=excel_data,
            file_name=f"RO_Readings_{month}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )



# -----------------------------
# DAILY READINGS PAGE
# -----------------------------
def page_readings():
    st.title("🧾 Daily RO Readings")

    df = get_readings()

    # Entry form
    st.subheader("Add / Update Reading")
    with st.form("add_reading_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=datetime.now().date())
            tds = st.number_input("TDS (ppm)", min_value=0.0, step=1.0)
        with c2:
            ph = st.number_input("pH", min_value=0.0, max_value=14.0, step=0.1)
            cond = st.number_input("Conductivity (µS/cm)", min_value=0.0, step=1.0)
        with c3:
            flow = st.number_input("Flow (m³/h)", min_value=0.0, step=0.1)
            prod = st.number_input("Daily Production (m³)", min_value=0.0, step=0.1)
        maint = st.text_input("Maintenance", "")
        notes = st.text_area("Notes", "")

        submitted = st.form_submit_button("💾 Save Reading")
    if submitted:
        conn = get_conn()
        cur = conn.cursor()
        # If a reading already exists for that date, update it, else insert
        cur.execute("SELECT id FROM readings WHERE d=%s", (d,))
        row = cur.fetchone()
        if row:
            cur.execute(
                """
                UPDATE readings
                SET tds=%s, ph=%s, conductivity=%s, flow_m3=%s, production=%s,
                    maintenance=%s, notes=%s
                WHERE id=%s
                """,
                (tds, ph, cond, flow, prod, maint, notes, row[0]),
            )
        else:
            cur.execute(
                """
                INSERT INTO readings
                    (d, tds, ph, conductivity, flow_m3, production, maintenance, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (d, tds, ph, cond, flow, prod, maint, notes),
            )
        conn.commit()
        cur.close()
        conn.close()
        st.success("Reading saved.")
        st.cache_data.clear()

    st.markdown("---")

    st.subheader("Edit Readings Table")
    df = get_readings()
    if len(df) == 0:
        st.info("No readings found yet.")
    else:
        editable = df.copy()
        editable["d"] = editable["d"].dt.date
        edited = st.data_editor(
            editable,
            num_rows="dynamic",
            use_container_width=True,
            key="readings_editor",
        )
        if st.button("💾 Save Table Changes", key="save_readings_table"):
            conn = get_conn()
            cur = conn.cursor()
            for _, row in edited.iterrows():
                cur.execute(
                    """
                    UPDATE readings
                    SET d=%s, tds=%s, ph=%s, conductivity=%s, flow_m3=%s,
                        production=%s, maintenance=%s, notes=%s
                    WHERE id=%s
                    """,
                    (
                        row["d"],
                        float(row.get("tds") or 0),
                        float(row.get("ph") or 0),
                        float(row.get("conductivity") or 0),
                        float(row.get("flow_m3") or 0),
                        float(row.get("production") or 0),
                        row.get("maintenance") or "",
                        row.get("notes") or "",
                        int(row["id"]),
                    ),
                )
            conn.commit()
            cur.close()
            conn.close()
            st.success("Readings table updated.")
            st.cache_data.clear()


# -----------------------------
# CHEMICAL MOVEMENTS PAGE
# -----------------------------
def page_chem_movements():
    st.title("🧪 Chemical Movements (IN / OUT)")

    chem_df = get_chemicals()
    mov_df = get_chemical_movements()

    # Entry form
    st.subheader("Record Chemical Movement")
    with st.form("chem_movement_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=datetime.now().date(), key="chem_date")
            if len(chem_df) > 0:
                name = st.selectbox("Chemical", chem_df["name"].tolist())
            else:
                name = st.text_input("Chemical Name (no chemicals in DB)")
        with c2:
            movement_type = st.selectbox("Movement Type", ["IN", "OUT"])
            qty = st.number_input("Quantity (kg)", min_value=0.0, step=0.1)
        with c3:
            remarks = st.text_input("Remarks", "")

        submitted = st.form_submit_button("💾 Save Movement")
    if submitted:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO chemical_movements (d, name, movement_type, qty, remarks)
            VALUES (%s,%s,%s,%s,%s)
            """,
            (d, name, movement_type, qty, remarks),
        )
        # update stock
        if movement_type == "IN":
            cur.execute(
                "UPDATE chemicals SET qty = qty + %s WHERE name=%s",
                (qty, name),
            )
        else:
            cur.execute(
                "UPDATE chemicals SET qty = qty - %s WHERE name=%s",
                (qty, name),
            )
        conn.commit()
        cur.close()
        conn.close()
        st.success("Chemical movement recorded and stock updated.")
        st.cache_data.clear()

    st.markdown("---")

    st.subheader("Edit Movements Table")
    mov_df = get_chemical_movements()
    if len(mov_df) == 0:
        st.info("No movements found yet.")
    else:
        editable = mov_df.copy()
        editable["d"] = editable["d"].dt.date
        edited = st.data_editor(
            editable,
            num_rows="dynamic",
            use_container_width=True,
            key="movements_editor",
        )
        if st.button("💾 Save Movements Table Changes", key="save_movements_table"):
            conn = get_conn()
            cur = conn.cursor()
            for _, row in edited.iterrows():
                cur.execute(
                    """
                    UPDATE chemical_movements
                    SET d=%s, name=%s, movement_type=%s, qty=%s, remarks=%s
                    WHERE id=%s
                    """,
                    (
                        row["d"],
                        row["name"],
                        row["movement_type"],
                        float(row.get("qty") or 0),
                        row.get("remarks") or "",
                        int(row["id"]),
                    ),
                )
            conn.commit()
            cur.close()
            conn.close()
            st.success("Chemical movements updated.")
            st.cache_data.clear()
# -----------------------------
# MAIN
# -----------------------------
def main():
    init_db()

    st.sidebar.title("Navigation")
    page = st.sidebar.radio("Go to", ["Dashboard", "Monthly Report", "Cartridge Filter", "Readings", "Chemical Movements"])

    if page == "Dashboard":
        page_dashboard()
    elif page == "Monthly Report":
        page_monthly_report()
    elif page == "Cartridge Filter":
        page_cartridge()
    elif page == "Readings":
        page_readings()
    else:
        page_chem_movements()

if __name__ == "__main__":
    main()
