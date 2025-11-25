# ============================================================
#   NPS – RO SYSTEM (UM Qasr) – EMERALD GREEN PRO UI
#   Backend: Neon PostgreSQL | Frontend: Streamlit
# ============================================================

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fpdf import FPDF

# ============================================================
# 1) GLOBAL SETTINGS – EMERALD GREEN THEME
# ============================================================

EMERALD_GREEN = "#1ABC9C"
DARK_GREEN = "#0E6655"
LIGHT_BG = "#ECF8F6"
WHITE = "#FFFFFF"
GREY = "#666666"

# Fixed list of chemicals used in Um Qasr RO
CHEMICAL_SETTINGS = {
    "HCL": {
        "display_name": "HCL (Hydrochloric Acid)",
        "min_level": 50,   # kg – reorder below this
        "max_level": 200   # kg – normal full stock
    },
    "BC": {
        "display_name": "BC (Biocide)",
        "min_level": 50,
        "max_level": 200
    },
    "Chlorine": {
        "display_name": "Chlorine",
        "min_level": 50,
        "max_level": 200
    },
}
ALLOWED_CHEMICALS = list(CHEMICAL_SETTINGS.keys())

st.set_page_config(
    page_title="NPS RO System – Um Qasr",
    layout="wide",
    page_icon="💧"
)

# Streamlit custom CSS theme
st.markdown(
    f"""
    <style>
        .stApp {{
            background-color: {LIGHT_BG} !important;
        }}
        .metric-card {{
            background-color: {WHITE};
            padding: 18px;
            border-radius: 12px;
            border-left: 6px solid {EMERALD_GREEN};
            box-shadow: 0 2px 6px rgba(0,0,0,0.1);
            margin-bottom: 14px;
        }}
        .section-header {{
            font-size: 22px;
            font-weight: 700;
            color: {DARK_GREEN};
            margin-top: 20px;
            margin-bottom: 10px;
        }}
        .sub-header {{
            font-size: 16px;
            font-weight: 600;
            color: {GREY};
            margin-top: 6px;
        }}
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# 2) NEON POSTGRESQL DATABASE CONNECTION
# ============================================================

DB_URL = (
    "postgresql://neondb_owner:"
    "npg_C4ghxK1yUcfw@"
    "ep-billowing-fog-agxbr2fc-pooler.c-2.eu-central-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)


def get_conn():
    """Create a PostgreSQL connection with RealDict cursor."""
    try:
        conn = psycopg2.connect(
            DB_URL,
            cursor_factory=RealDictCursor,
            connect_timeout=10,
            sslmode="require",
        )
        return conn
    except Exception as e:
        st.error(f"❌ Database Connection Failed: {e}")
        return None


# ============================================================
# 3) INITIALIZE / CREATE TABLES (POSTGRESQL)
# ============================================================

def init_postgres():
    conn = get_conn()
    if not conn:
        return
    cur = conn.cursor()

    # ---- readings table ----
    cur.execute(
        """
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
        """
    )

    # ---- cartridge table ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cartridge (
            id SERIAL PRIMARY KEY,
            d DATE,
            dp DOUBLE PRECISION,
            remarks TEXT,
            is_change INTEGER DEFAULT 0,
            change_cost DOUBLE PRECISION DEFAULT 0
        );
        """
    )

    # ---- chemicals table ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chemicals (
            name TEXT PRIMARY KEY,
            qty DOUBLE PRECISION,
            unit_cost DOUBLE PRECISION DEFAULT 0
        );
        """
    )

    # Base chemicals (HCL, BC, Chlorine)
    cur.execute(
        """
        INSERT INTO chemicals (name, qty, unit_cost)
        VALUES
            ('Chlorine', 0, 0),
            ('HCL', 0, 0),
            ('BC', 0, 0)
        ON CONFLICT (name) DO NOTHING;
        """
    )
    # Clean wrong rows (like 'name')
    cur.execute(
        """
        DELETE FROM chemicals
        WHERE name NOT IN ('HCL','BC','Chlorine');
        """
    )

    # ---- chemical movements ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chemical_movements (
            id SERIAL PRIMARY KEY,
            d DATE,
            name TEXT,
            movement_type TEXT,
            qty DOUBLE PRECISION,
            remarks TEXT
        );
        """
    )

    conn.commit()
    conn.close()


# ============================================================
# 4) DATA ACCESS FUNCTIONS
# ============================================================

def add_reading(d, tds, ph, cond, flow, prod, maint, notes):
    conn = get_conn()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO readings (d, tds, ph, conductivity, flow_m3, production, maintenance, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (d, tds, ph, cond, flow, prod, maint, notes),
    )
    conn.commit()
    conn.close()


def get_readings():
    conn = get_conn()
    if not conn:
        return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM readings ORDER BY d", conn)
    conn.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]
    return df


def add_cartridge(d, dp, remarks, is_change, cost):
    conn = get_conn()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO cartridge (d, dp, remarks, is_change, change_cost)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (d, dp, remarks, is_change, cost),
    )
    conn.commit()
    conn.close()


def get_cartridge():
    conn = get_conn()
    if not conn:
        return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM cartridge ORDER BY d", conn)
    conn.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]
    return df


def get_chemicals():
    conn = get_conn()
    if not conn:
        return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM chemicals", conn)
    conn.close()
    if len(df) > 0:
        df = df[df["name"].isin(ALLOWED_CHEMICALS)]
    return df


def update_chemical_cost(name, cost):
    conn = get_conn()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute("UPDATE chemicals SET unit_cost = %s WHERE name = %s", (cost, name))
    conn.commit()
    conn.close()


def record_chemical_movement(d, name, mov, qty, remarks):
    conn = get_conn()
    if not conn:
        return
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO chemical_movements (d, name, movement_type, qty, remarks)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (d, name, mov, qty, remarks),
    )
    # Update stock
    if mov == "IN":
        cur.execute("UPDATE chemicals SET qty = qty + %s WHERE name = %s", (qty, name))
    else:
        cur.execute(
            "UPDATE chemicals SET qty = GREATEST(qty - %s, 0) WHERE name = %s",
            (qty, name),
        )
    conn.commit()
    conn.close()


def get_chemical_movements():
    conn = get_conn()
    if not conn:
        return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM chemical_movements ORDER BY d", conn)
    conn.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]
    return df


# ============================================================
# 5) UI HELPERS
# ============================================================

def kpi_card(title, value, color=EMERALD_GREEN):
    st.markdown(
        f"""
        <div class="metric-card" style="border-left-color:{color};">
            <h4 style="margin:0; color:{GREY}; font-size:15px;">{title}</h4>
            <h2 style="margin:0; color:{DARK_GREEN};">{value}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(text):
    st.markdown(f"<div class='section-header'>{text}</div>", unsafe_allow_html=True)


def compute_compliance(df):
    """TDS <= 50 ppm AND 6.5 <= pH <= 8.5"""
    if len(df) == 0:
        return 0.0, 0, 0
    cond_ok = (df["tds"] <= 50) & (df["ph"].between(6.5, 8.5))
    total = len(df)
    good = int(cond_ok.sum())
    bad = total - good
    comp = (good / total) * 100 if total else 0.0
    return round(comp, 1), good, bad


def render_dp_gauge(dp_value):
    """DP gauge 0–10 bar with thresholds at 1, 2, 3 bar."""
    dp = max(0, min(dp_value, 10))

    if dp < 1:
        needle_color = "#1ABC9C"  # green
    elif dp < 2:
        needle_color = "#F1C40F"  # yellow
    elif dp < 3:
        needle_color = "#E67E22"  # orange
    else:
        needle_color = "#E74C3C"  # red

    fig, ax = plt.subplots(figsize=(2.8, 2.8))
    ax.set_xlim(-1, 1)
    ax.set_ylim(-1, 1)
    ax.axis("off")

    ang = np.linspace(-0.75 * np.pi, 0.75 * np.pi, 300)
    ax.plot(np.cos(ang), np.sin(ang), linewidth=3, color=DARK_GREEN)

    angle = -0.75 * np.pi + (dp / 10) * (1.5 * np.pi)
    nx = 0.75 * np.cos(angle)
    ny = 0.75 * np.sin(angle)
    ax.plot([0, nx], [0, ny], linewidth=4, color=needle_color)

    ax.plot(0, 0, "o", markersize=10, color=EMERALD_GREEN)
    ax.text(0, -0.25, f"{dp:.2f} bar", ha="center", fontsize=11, color=DARK_GREEN)
    ax.text(0, -0.42, "DP", ha="center", fontsize=9, color=GREY)

    st.pyplot(fig)


# ============================================================
# 6) DASHBOARD PAGE
# ============================================================

def page_dashboard():
    st.markdown(
        "<h1 style='color:#0E6655;'>RO System Dashboard – Um Qasr</h1>",
        unsafe_allow_html=True,
    )
    st.caption("Emerald Corporate Dashboard • Water, Chemicals, Cartridge & Cost")

    df = get_readings()
    chem = get_chemicals()
    cart = get_cartridge()

    last_tds = "---"
    last_ph_str = "---"
    last_ph_val = None
    total_records = len(df)
    comp_val = "N/A"
    good_days = bad_days = 0
    prod30 = 0.0

    if len(df) > 0:
        last = df.iloc[-1]
        last_tds = f"{last['tds']:.1f}"
        last_ph_val = float(last["ph"])
        last_ph_str = f"{last_ph_val:.2f}"

        df30 = df[df["d"] >= datetime.now() - pd.Timedelta(days=30)]
        if len(df30) > 0:
            comp, good_days, bad_days = compute_compliance(df30)
            comp_val = f"{comp:.1f}%"
            prod30 = df30["production"].sum()
    else:
        df30 = pd.DataFrame()

    # Color for pH KPI
    ph_color = "#E74C3C"  # red by default
    if last_ph_val is not None:
        if 7.0 <= last_ph_val <= 8.0:
            ph_color = "#27AE60"  # green
        elif 6.5 <= last_ph_val < 7.0 or 8.0 < last_ph_val <= 8.5:
            ph_color = "#F1C40F"  # amber

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("Unit Capacity", "10 m³/hr")
    with col2:
        kpi_card("Last TDS", last_tds)
    with col3:
        kpi_card("Last pH", last_ph_str, color=ph_color)
    with col4:
        kpi_card("Total Readings", total_records)

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        kpi_card("30-day Compliance", comp_val)
    with col6:
        kpi_card("In-Spec Days (30d)", good_days)
    with col7:
        kpi_card("Out-of-Spec (30d)", bad_days)
    with col8:
        kpi_card("30-day Production", f"{prod30:.1f} m³")

    # ---------- WATER & PRODUCTION ----------
    col_w1, col_w2 = st.columns(2)
    with col_w1:
        section_title("Water Quality Trend (Last 30 Days)")
        if len(df30) > 0:
            fig, ax = plt.subplots(figsize=(6, 3))
            ax.plot(df30["d"], df30["tds"], label="TDS (ppm)")
            ax.plot(df30["d"], df30["ph"], label="pH")
            ax.plot(df30["d"], df30["conductivity"], label="Cond (µS/cm)")
            ax.legend()
            ax.grid(alpha=0.3)
            plt.xticks(rotation=45)
            st.pyplot(fig)
        else:
            st.info("No readings for last 30 days. Add daily readings to see trends.")

    with col_w2:
        section_title("Daily Production (Last 30 Days)")
        if len(df30) > 0:
            fig2, ax2 = plt.subplots(figsize=(6, 3))
            ax2.bar(df30["d"], df30["production"])
            ax2.set_ylabel("m³/day")
            ax2.grid(alpha=0.3, axis="y")
            plt.xticks(rotation=45)
            st.pyplot(fig2)
        else:
            st.info("No production data yet.")

    # ---------- DP + CHEMICAL SNAPSHOT ----------
    col_d1, col_d2 = st.columns(2)
    with col_d1:
        section_title("Cartridge Filter – DP Status")
        if len(cart) == 0:
            st.info("No cartridge records yet.")
            kpi_card("Latest DP", "0.00 bar")
        else:
            latest_dp = float(cart["dp"].iloc[-1])
            kpi_card("Latest DP", f"{latest_dp:.2f} bar")
            render_dp_gauge(latest_dp)

            if latest_dp < 1:
                st.success("🟢 DP < 1 bar – Cartridge OK.")
            elif latest_dp < 2:
                st.warning("🟡 1 ≤ DP < 2 bar – Monitor filter, plan check.")
            elif latest_dp < 3:
                st.warning("🟠 2 ≤ DP < 3 bar – High DP, prepare to change soon.")
            else:
                st.error("🔴 DP ≥ 3 bar – Change cartridge immediately.")

    with col_d2:
        section_title("Chemical Stock Snapshot")
        if len(chem) == 0:
            st.info("No chemicals found. Check database connection.")
        else:
            chem_df = chem.copy()
            chem_df["qty"] = pd.to_numeric(chem_df["qty"], errors="coerce").fillna(0.0)
            chem_df["unit_cost"] = pd.to_numeric(
                chem_df["unit_cost"], errors="coerce"
            ).fillna(0.0)
            chem_df["display_name"] = chem_df["name"].map(
                lambda n: CHEMICAL_SETTINGS.get(n, {}).get("display_name", n)
            )
            chem_df["stock_value"] = chem_df["qty"] * chem_df["unit_cost"]

            if chem_df["qty"].sum() > 0:
                fig3, ax3 = plt.subplots(figsize=(4, 4))
                ax3.pie(
                    chem_df["qty"],
                    labels=chem_df["display_name"],
                    autopct="%1.1f%%",
                    wedgeprops={"width": 0.45},
                    startangle=90,
                )
                ax3.set_title("Stock Distribution (kg)")
                st.pyplot(fig3)
            else:
                st.info("All chemicals currently at 0 kg.")

            total_val = float(chem_df["stock_value"].sum())
            kpi_card("Total Chemical Stock Value", f"{total_val:,.0f}")

    # ---------- CHEMICAL STATUS TABLE ----------
    section_title("Chemical Status & Alerts")
    if len(chem) == 0:
        st.info("No chemicals data to display.")
    else:
        chem_df = chem.copy()
        chem_df["qty"] = pd.to_numeric(chem_df["qty"], errors="coerce").fillna(0.0)
        chem_df["unit_cost"] = pd.to_numeric(
            chem_df["unit_cost"], errors="coerce"
        ).fillna(0.0)
        chem_df["min_level"] = chem_df["name"].map(
            lambda n: CHEMICAL_SETTINGS.get(n, {}).get("min_level", 0)
        )
        chem_df["max_level"] = chem_df["name"].map(
            lambda n: CHEMICAL_SETTINGS.get(n, {}).get("max_level", 0)
        )
        chem_df["display_name"] = chem_df["name"].map(
            lambda n: CHEMICAL_SETTINGS.get(n, {}).get("display_name", n)
        )

        def status_from_row(row):
            if row["qty"] <= 0:
                return "EMPTY"
            if row["qty"] < row["min_level"]:
                return "LOW"
            if row["qty"] > row["max_level"]:
                return "HIGH"
            return "OK"

        chem_df["status"] = chem_df.apply(status_from_row, axis=1)
        chem_df["stock_value"] = chem_df["qty"] * chem_df["unit_cost"]

        disp = chem_df[
            [
                "display_name",
                "qty",
                "min_level",
                "max_level",
                "unit_cost",
                "stock_value",
                "status",
            ]
        ].rename(
            columns={
                "display_name": "Chemical",
                "qty": "Qty (kg)",
                "min_level": "Min level (kg)",
                "max_level": "Max level (kg)",
                "unit_cost": "Unit cost",
                "stock_value": "Stock value",
            }
        )

        st.dataframe(disp, use_container_width=True)

        for _, row in chem_df.iterrows():
            nm = row["display_name"]
            qty = float(row["qty"])
            min_level = float(row["min_level"])
            max_level = float(row["max_level"])
            status = row["status"]

            if status == "EMPTY":
                st.error(f"🔴 {nm}: stock is EMPTY (0 kg) – urgent reorder.")
            elif status == "LOW":
                st.warning(
                    f"🟠 {nm}: low stock ({qty:.1f} kg, below {min_level:.1f} kg) – plan reorder."
                )
            elif status == "HIGH":
                st.info(
                    f"ℹ️ {nm}: above normal level ({qty:.1f} kg, normal up to {max_level:.1f} kg)."
                )
            else:
                st.success(f"🟢 {nm}: stock OK ({qty:.1f} kg).")


# ============================================================
# 7) PAGE – ADD DAILY READING
# ============================================================

def page_add_reading():
    st.markdown(
        "<h1 style='color:#0E6655;'>Daily RO Reading – Um Qasr</h1>",
        unsafe_allow_html=True,
    )
    st.caption("Record water quality, production, and maintenance for the RO unit.")

    with st.form("daily_reading_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=date.today())
        with c2:
            tds = st.number_input("TDS (ppm)", min_value=0.0, step=0.1)
        with c3:
            # pH from 1 to 14
            ph = st.number_input("pH", min_value=1.0, max_value=14.0, step=0.01)

        c4, c5, c6 = st.columns(3)
        with c4:
            conductivity = st.number_input(
                "Conductivity (µS/cm)", min_value=0.0, step=1.0
            )
        with c5:
            flow_m3 = st.number_input("Flow (m³/hr)", min_value=0.0, step=0.1)
        with c6:
            production = st.number_input(
                "Production Today (m³)", min_value=0.0, step=0.1
            )

        st.markdown("<div class='sub-header'>Maintenance / Notes</div>", unsafe_allow_html=True)
        maintenance = st.text_area("Maintenance done today", height=80)
        notes = st.text_area("Notes / alarms / comments", height=80)

        submitted = st.form_submit_button("💾 Save Daily Reading")
        if submitted:
            add_reading(
                str(d), tds, ph, conductivity, flow_m3, production, maintenance, notes
            )
            st.success("✅ Daily RO reading saved successfully.")
            st.info("You can see it now in Dashboard and in the table below.")

    st.markdown("---")
    section_title("Last 10 Readings")

    df = get_readings()
    if len(df) > 0:
        st.dataframe(df.tail(10), use_container_width=True)
    else:
        st.info("No readings recorded yet.")


# ============================================================
# 8) PAGE – CHEMICALS (STOCK, COST & MOVEMENTS)
# ============================================================

def page_chemicals():
    st.markdown(
        "<h1 style='color:#0E6655;'>Chemicals – Stock, Cost & Movements</h1>",
        unsafe_allow_html=True,
    )
    st.caption("Track chemical stock, update unit costs, and log IN / OUT movements.")

    chem_df = get_chemicals()
    mov_df = get_chemical_movements()

    tab1, tab2, tab3 = st.tabs(["📦 Stock & Cost", "➕ Record IN / OUT", "📜 Movements History"])

    # ---------- TAB 1 ----------
    with tab1:
        section_title("Current Stock and Value")

        if len(chem_df) == 0:
            st.info("No chemicals found in database.")
        else:
            chem_df = chem_df.copy()
            chem_df["qty"] = pd.to_numeric(
                chem_df["qty"], errors="coerce"
            ).fillna(0.0)
            chem_df["unit_cost"] = pd.to_numeric(
                chem_df["unit_cost"], errors="coerce"
            ).fillna(0.0)

            chem_df["min_level"] = chem_df["name"].map(
                lambda n: CHEMICAL_SETTINGS.get(n, {}).get("min_level", 0)
            )
            chem_df["max_level"] = chem_df["name"].map(
                lambda n: CHEMICAL_SETTINGS.get(n, {}).get("max_level", 0)
            )
            chem_df["display_name"] = chem_df["name"].map(
                lambda n: CHEMICAL_SETTINGS.get(n, {}).get("display_name", n)
            )

            def status_from_row(row):
                if row["qty"] <= 0:
                    return "EMPTY"
                if row["qty"] < row["min_level"]:
                    return "LOW"
                if row["qty"] > row["max_level"]:
                    return "HIGH"
                return "OK"

            chem_df["status"] = chem_df.apply(status_from_row, axis=1)
            chem_df["stock_value"] = chem_df["qty"] * chem_df["unit_cost"]

            total_stock_value = float(chem_df["stock_value"].sum())
            total_qty = float(chem_df["qty"].sum())
            low_count = int(
                (chem_df["status"] == "LOW").sum()
                + (chem_df["status"] == "EMPTY").sum()
            )

            col_k1, col_k2, col_k3 = st.columns(3)
            with col_k1:
                kpi_card("Total Stock (kg)", f"{total_qty:,.1f}")
            with col_k2:
                kpi_card("Total Stock Value", f"{total_stock_value:,.0f}")
            with col_k3:
                kpi_card("Low / Empty Items", f"{low_count}")

            disp = chem_df[
                [
                    "display_name",
                    "qty",
                    "min_level",
                    "max_level",
                    "unit_cost",
                    "stock_value",
                    "status",
                ]
            ].rename(
                columns={
                    "display_name": "Chemical",
                    "qty": "Qty (kg)",
                    "min_level": "Min level (kg)",
                    "max_level": "Max level (kg)",
                    "unit_cost": "Unit cost",
                    "stock_value": "Stock value",
                }
            )
            st.dataframe(disp, use_container_width=True)

            # Alerts
            for _, row in chem_df.iterrows():
                nm = row["display_name"]
                qty = float(row["qty"])
                min_level = float(row["min_level"])
                max_level = float(row["max_level"])
                status = row["status"]

                if status == "EMPTY":
                    st.error(f"🔴 {nm}: stock is EMPTY (0 kg) – urgent reorder.")
                elif status == "LOW":
                    st.warning(
                        f"🟠 {nm}: low stock ({qty:.1f} kg, below {min_level:.1f} kg) – plan reorder."
                    )
                elif status == "HIGH":
                    st.info(
                        f"ℹ️ {nm}: above normal level ({qty:.1f} kg, normal up to {max_level:.1f} kg)."
                    )
                else:
                    st.success(f"🟢 {nm}: stock OK ({qty:.1f} kg).")

            # Bar + donut charts
            section_title("Stock by Chemical (kg)")
            fig, ax = plt.subplots(figsize=(5, 3))
            ax.bar(chem_df["display_name"], chem_df["qty"])
            ax.set_ylabel("Qty (kg)")
            ax.grid(alpha=0.3, axis="y")
            st.pyplot(fig)

            if chem_df["qty"].sum() > 0:
                section_title("Stock Distribution")
                fig2, ax2 = plt.subplots(figsize=(4, 4))
                ax2.pie(
                    chem_df["qty"],
                    labels=chem_df["display_name"],
                    autopct="%1.1f%%",
                    wedgeprops={"width": 0.45},
                    startangle=90,
                )
                st.pyplot(fig2)

            # ---- Unit cost update ----
            st.markdown(
                "<div class='sub-header'>Update Unit Cost (per kg)</div>",
                unsafe_allow_html=True,
            )

            c1, c2, c3 = st.columns([1, 1, 0.6])
            with c1:
                # INLINE 3 options instead of dropdown
                chem_name = st.radio(
                    "Chemical",
                    ALLOWED_CHEMICALS,
                    horizontal=True,
                )
            with c2:
                if chem_name in chem_df["name"].values:
                    current_cost = float(
                        chem_df.loc[chem_df["name"] == chem_name, "unit_cost"].iloc[0]
                    )
                else:
                    current_cost = 0.0
                new_cost = st.number_input(
                    "Unit Cost (per kg)",
                    min_value=0.0,
                    step=0.1,
                    value=current_cost,
                )
            with c3:
                if st.button("💾 Save Cost"):
                    update_chemical_cost(chem_name, new_cost)
                    st.success(f"✅ Unit cost updated for {chem_name}.")

    # ---------- TAB 2 ----------
    with tab2:
        section_title("Record Chemical IN / OUT")

        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=date.today())
        with c2:
            name = st.radio(
                "Chemical",
                ALLOWED_CHEMICALS,
                horizontal=True,
            )
        with c3:
            movement_type = st.selectbox("Movement Type", ["IN", "OUT"])

        qty = st.number_input("Quantity (kg)", min_value=0.0, step=0.1)
        remarks = st.text_input(
            "Remarks / reference (invoice, batch, etc.)",
        )

        if st.button("💾 Save Movement"):
            if qty <= 0:
                st.error("Quantity must be greater than 0.")
            else:
                record_chemical_movement(str(d), name, movement_type, qty, remarks)
                st.success(f"✅ {movement_type} movement recorded for {name}.")

    # ---------- TAB 3 ----------
    with tab3:
        section_title("Chemical Movements History")

        if len(mov_df) == 0:
            st.info("No chemical movements recorded yet.")
        else:
            mov_df = mov_df.copy()
            mov_df = mov_df[mov_df["name"].isin(ALLOWED_CHEMICALS)]

            with st.expander("🔍 Filter history"):
                sel_chems = st.multiselect(
                    "Chemical", ALLOWED_CHEMICALS, default=ALLOWED_CHEMICALS
                )
                col_f1, col_f2 = st.columns(2)
                with col_f1:
                    from_date = st.date_input(
                        "From date",
                        value=mov_df["d"].min().date()
                        if len(mov_df) > 0
                        else date.today(),
                    )
                with col_f2:
                    to_date = st.date_input(
                        "To date",
                        value=mov_df["d"].max().date()
                        if len(mov_df) > 0
                        else date.today(),
                    )

            mask = (mov_df["name"].isin(sel_chems)) & (
                mov_df["d"].dt.date >= from_date
            ) & (mov_df["d"].dt.date <= to_date)
            mov_f = mov_df[mask].sort_values("d")

            st.dataframe(mov_f, use_container_width=True)

            section_title("Last 30 Days – Usage Summary (OUT only)")
            last_30 = mov_df[mov_df["d"] >= (datetime.now() - pd.Timedelta(days=30))]
            last_30_out = last_30[last_30["movement_type"] == "OUT"]

            if len(last_30_out) > 0:
                summary = last_30_out.groupby("name")["qty"].sum().reset_index()
                summary["display_name"] = summary["name"].map(
                    lambda n: CHEMICAL_SETTINGS.get(n, {}).get("display_name", n)
                )
                summary = summary[["display_name", "qty"]].rename(
                    columns={
                        "display_name": "Chemical",
                        "qty": "Qty OUT (kg, last 30 days)",
                    }
                )
                st.dataframe(summary, use_container_width=True)
            else:
                st.info("No OUT movements in the last 30 days.")


# ============================================================
# 9) PAGE – CARTRIDGE FILTER (DP MONITORING)
# ============================================================

def page_cartridge():
    st.markdown(
        "<h1 style='color:#0E6655;'>Cartridge Filter – DP Monitoring</h1>",
        unsafe_allow_html=True,
    )
    st.caption("Monitor differential pressure (DP) and log cartridge actions.")

    st.markdown("<div class='sub-header'>Enter Pressure Readings</div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        before = st.number_input(
            "Pressure BEFORE Filter (bar)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,
            step=0.1,
        )
    with col2:
        after = st.number_input(
            "Pressure AFTER Filter (bar)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,
            step=0.1,
        )

    if after > before:
        st.warning("After-pressure cannot be higher than before. Check readings.")
    dp = max(before - after, 0.0)

    kpi_card("Differential Pressure (DP)", f"{dp:.2f} bar")

    st.markdown("<div class='sub-header'>DP Gauge (0–10 bar)</div>", unsafe_allow_html=True)
    render_dp_gauge(dp)

    if dp < 1:
        st.success("🟢 DP < 1 bar – Cartridge OK.")
    elif dp < 2:
        st.warning("🟡 1 ≤ DP < 2 bar – Monitor filter, plan check.")
    elif dp < 3:
        st.warning("🟠 2 ≤ DP < 3 bar – High DP, prepare to change soon.")
    else:
        st.error("🔴 DP ≥ 3 bar – Change cartridge immediately.")

    st.markdown("---")
    section_title("Save Cartridge Record")

    c1, c2 = st.columns(2)
    with c1:
        d = st.date_input("Date", value=date.today())
    with c2:
        is_change = st.checkbox("Cartridge replaced during this visit?")

    remarks = st.text_input("Remarks (change / inspection / cleaning)")
    change_cost = 0.0
    if is_change:
        change_cost = st.number_input(
            "Replacement Cost", min_value=0.0, step=1.0, value=0.0
        )

    if st.button("💾 Save Cartridge Log"):
        add_cartridge(str(d), dp, remarks, int(is_change), change_cost)
        st.success("✅ Cartridge filter record saved successfully.")

    st.markdown("---")
    section_title("Cartridge Filter History")

    hist = get_cartridge()
    if len(hist) == 0:
        st.info("No cartridge history available.")
    else:
        st.dataframe(hist, use_container_width=True)


# ============================================================
# 10) PDF HELPERS, MONTHLY & MAINTENANCE PAGES
# (unchanged from previous working version – kept for reports)
# ============================================================

# ... keep your existing create_pdf, create_maintenance_pdf,
# page_monthly_report, page_maintenance_report here (same as last version) ...
# To save space I’m not repeating them, but you can copy from the
# last full code I sent if needed.

# ============================================================
# 11) MAIN ROUTER
# ============================================================

def main():
    init_postgres()

    st.sidebar.markdown(
        f"""
        <h2 style="color:{DARK_GREEN};">Nile Projects Service</h2>
        <p style="color:{GREY}; font-size:13px;">
            RO System – Um Qasr Port<br>
            10 m³/hr Unit • Emerald ISO 14001 Theme
        </p>
        <hr>
        """,
        unsafe_allow_html=True,
    )

    menu = st.sidebar.radio(
        "Menu",
        [
            "Dashboard",
            "Add Daily Reading",
            "Chemicals",
            "Cartridge Filter",
            "Monthly Report",
            "Maintenance Report",
        ],
    )

    if menu == "Dashboard":
        page_dashboard()
    elif menu == "Add Daily Reading":
        page_add_reading()
    elif menu == "Chemicals":
        page_chemicals()
    elif menu == "Cartridge Filter":
        page_cartridge()
    elif menu == "Monthly Report":
        page_monthly_report()
    elif menu == "Maintenance Report":
        page_maintenance_report()


if __name__ == "__main__":
    main()
