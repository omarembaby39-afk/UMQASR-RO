# ============================================================
#   NPS – RO SYSTEM (UM QASR) – PROFESSIONAL EMERALD GREEN UI
#   Backend: Neon PostgreSQL | Frontend: Streamlit Pro Layout
#   PART 1 / 4
# ============================================================

import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, date
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from fpdf import FPDF
import os

# ============================================================
# 1) GLOBAL SETTINGS – EMERALD GREEN THEME
# ============================================================

EMERALD_GREEN = "#1ABC9C"
DARK_GREEN = "#0E6655"

# Minimum stock thresholds (kg) for each chemical
MIN_STOCK = {
    "Sodium Hypochlorite (Chlorine)": 50,
    "Hydrochloric Acid (HCL)": 30,
    "Antiscalant PC-391": 25,
}

LIGHT_BG = "#ECF8F6"
WHITE = "#FFFFFF"
GREY = "#666666"

st.set_page_config(
    page_title="NPS RO System – Um Qasr",
    layout="wide",
    page_icon="💧"
)

# Streamlit custom CSS theme (Emerald Corporate Green)
st.markdown(f"""
    <style>
        .stApp {{
            background-color: {LIGHT_BG} !important;
        }}
        .css-18ni7ap {{
            background-color: {WHITE} !important;
        }}
        .stProgress > div > div {{
            background-color: {EMERALD_GREEN} !important;
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
        .card {{
            background: #ffffff;
            padding: 20px;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            margin-bottom: 20px;
        }}
    </style>
""", unsafe_allow_html=True)


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
            sslmode="require"
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

    # ---- cartridge table ----
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

    # ---- chemicals ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chemicals (
            name TEXT PRIMARY KEY,
            qty DOUBLE PRECISION,
            unit_cost DOUBLE PRECISION DEFAULT 0
        );
    """)

    # Insert base chemicals if missing (Um Qasr RO)
    cur.execute("""
        INSERT INTO chemicals (name, qty, unit_cost)
        VALUES
            ('Sodium Hypochlorite (Chlorine)', 0, 0),
            ('Hydrochloric Acid (HCL)', 0, 0),
            ('Antiscalant PC-391', 0, 0)
        ON CONFLICT (name) DO NOTHING;
    """)
    # ---- chemicals ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS chemicals (
            name TEXT PRIMARY KEY,
            qty DOUBLE PRECISION,
            unit_cost DOUBLE PRECISION DEFAULT 0
        );
    """)

    # Insert base chemicals if missing
    cur.execute("""
        INSERT INTO chemicals (name, qty, unit_cost)
        VALUES
            ('Chlorine', 0, 0),
            ('HCL', 0, 0),
            ('BC', 0, 0)
        ON CONFLICT (name) DO NOTHING;
    """)

    # ---- chemical movements ----
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
    conn.close()


# ============================================================
# 4) DATA ACCESS FUNCTIONS (POSTGRESQL)
# ============================================================

def add_reading(d, tds, ph, cond, flow, prod, maint, notes):
    conn = get_conn()
    if not conn: return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO readings (d, tds, ph, conductivity, flow_m3, production, maintenance, notes)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (d, tds, ph, cond, flow, prod, maint, notes))
    conn.commit()
    conn.close()


def get_readings():
    """Return all daily readings as a DataFrame (sorted by date)."""
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
    if not conn: return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO cartridge (d, dp, remarks, is_change, change_cost)
        VALUES (%s, %s, %s, %s, %s)
    """, (d, dp, remarks, is_change, cost))
    conn.commit()
    conn.close()


def get_cartridge():
    """Return all cartridge DP records as a DataFrame sorted by date."""
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
    if not conn: return pd.DataFrame()
    df = pd.read_sql("SELECT * FROM chemicals", conn)
    conn.close()
    return df


def update_chemical_cost(name, cost):
    conn = get_conn()
    if not conn: return
    cur = conn.cursor()
    cur.execute("UPDATE chemicals SET unit_cost = %s WHERE name = %s", (cost, name))
    conn.commit()
    conn.close()


def record_chemical_movement(d, name, mov, qty, remarks):
    conn = get_conn()
    if not conn: return
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO chemical_movements (d, name, movement_type, qty, remarks)
        VALUES (%s, %s, %s, %s, %s)
    """, (d, name, mov, qty, remarks))

    # Update stock
    if mov == "IN":
        cur.execute("UPDATE chemicals SET qty = qty + %s WHERE name = %s", (qty, name))
    else:
        cur.execute("UPDATE chemicals SET qty = GREATEST(qty - %s, 0) WHERE name = %s", (qty, name))

    conn.commit()
    conn.close()


def get_chemical_movements():
    """Return all chemical movement records as a DataFrame sorted by date."""
    conn = get_conn()
    if not conn:
        return pd.DataFrame()

    df = pd.read_sql("SELECT * FROM chemical_movements ORDER BY d", conn)
    conn.close()

    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"], errors="coerce")
        df = df[df["d"].notna()]

    return df


def kpi_card(title, value, color=EMERALD_GREEN):
    st.markdown(
        f"""
        <div class="metric-card" style="border-left-color:{color};">
            <h4 style="margin:0; color:{GREY}; font-size:16px;">{title}</h4>
            <h2 style="margin:0; color:{DARK_GREEN};">{value}</h2>
        </div>
        """,
        unsafe_allow_html=True,
    )


def section_title(text):
    st.markdown(f'<div class="section-header">{text}</div>', unsafe_allow_html=True)


# ============================================================
# 6) DP GAUGE (EMERALD GREEN)
# ============================================================

def render_dp_gauge(dp_value):
    """Beautiful emerald-green DP gauge (0–10 bar)."""

    dp = max(0, min(dp_value, 10))

    # Colors
    if dp < 1:
        needle_color = "#1ABC9C"     # emerald green
    elif dp <= 4:
        needle_color = "#F39C12"     # amber warning
    else:
        needle_color = "#E74C3C"     # red danger

    fig, ax = plt.subplots(figsize=(2.6, 2.6))
    ax.set_xlim(-1, 1)
    ax.set_ylim(-1, 1)
    ax.axis("off")

    # Arc
    ang = np.linspace(-0.75 * np.pi, 0.75 * np.pi, 300)
    ax.plot(np.cos(ang), np.sin(ang), linewidth=3, color=DARK_GREEN)

    # Needle
    angle = -0.75 * np.pi + (dp / 10) * (1.5 * np.pi)
    nx = 0.75 * np.cos(angle)
    ny = 0.75 * np.sin(angle)
    ax.plot([0, nx], [0, ny], linewidth=4, color=needle_color)

    ax.plot(0, 0, "o", markersize=10, color=EMERALD_GREEN)
    ax.text(0, -0.25, f"{dp:.2f} bar", ha="center", fontsize=11, color=DARK_GREEN)
    ax.text(0, -0.42, "DP", ha="center", fontsize=9, color=GREY)

    st.pyplot(fig)


# ============================================================
# 7) DASHBOARD PAGE — EMERALD UI
# ============================================================

def page_dashboard():
    st.markdown("<h1 style='color:#0E6655;'>RO System Dashboard – Um Qasr</h1>", unsafe_allow_html=True)
    st.caption("Emerald Corporate UI • ISO 14001 Environmental Theme")

    df = get_readings()
    chem = get_chemicals()
    cart = get_cartridge()

    # ----------------------------------------------------------
    # ❇ TOP ROW METRICS
    # ----------------------------------------------------------

    if len(df) == 0:
        kpi_card("Unit Capacity", "10 m³/hr")
        st.info("No readings recorded yet.")
        return

    last = df.iloc[-1]
    last_tds = f"{last['tds']:.1f}"
    last_ph = f"{last['ph']:.2f}"

    df30 = df[df['d'] >= datetime.now() - pd.Timedelta(days=30)]
    if len(df30) > 0:
        cond_ok = (df30["tds"] <= 50) & (df30["ph"].between(6.5, 8.5))
        compliance = (cond_ok.sum() / len(df30)) * 100
        comp_val = f"{compliance:.1f}%"
        good_days = cond_ok.sum()
        bad_days = len(df30) - good_days
        prod30 = df30["production"].sum()
    else:
        comp_val, good_days, bad_days, prod30 = "0%", 0, 0, 0

    col1, col2, col3, col4 = st.columns(4)
    with col1: kpi_card("Unit Capacity", "10 m³/hr")
    with col2: kpi_card("Last TDS", last_tds)
    with col3: kpi_card("Last pH", last_ph)
    with col4: kpi_card("Total Records", len(df))

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        kpi_card("30-day Compliance", comp_val)
    with col6:
        kpi_card("In-Spec Days", good_days)
    with col7:
        kpi_card("Out-of-Spec", bad_days)
    with col8:
        kpi_card("30-d Production", f"{prod30:.1f} m³")

    # ----------------------------------------------------------
    # ❇ WATER QUALITY CHART
    # ----------------------------------------------------------

    section_title("Water Quality Trend (Last 30 Days)")

    if len(df30) > 0:
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.plot(df30["d"], df30["tds"], label="TDS (ppm)", color="#1ABC9C", linewidth=2)
        ax.plot(df30["d"], df30["ph"], label="pH", color="#0E6655", linewidth=2)
        ax.plot(df30["d"], df30["conductivity"], label="Conductivity", color="#117A65", linewidth=2)

        ax.legend()
        plt.xticks(rotation=45)
        ax.grid(alpha=0.3)
        st.pyplot(fig)
    else:
        st.info("No data for last 30 days.")

    # ----------------------------------------------------------
    # ❇ PRODUCTION BAR CHART
    # ----------------------------------------------------------

    section_title("Daily Production – Last 30 Days")

    if len(df30) > 0:
        fig2, ax2 = plt.subplots(figsize=(6, 3))
        ax2.bar(df30["d"], df30["production"], color=EMERALD_GREEN)
        ax2.set_ylabel("m³")
        plt.xticks(rotation=45)
        ax2.grid(alpha=0.3)
        st.pyplot(fig2)
    else:
        st.info("No production data.")

    # ----------------------------------------------------------
    # ❇ CARTRIDGE DP GAUGE
    # ----------------------------------------------------------

    section_title("Cartridge Filter DP Gauge")

    if len(cart) == 0:
        st.info("No cartridge records.")
    else:
        latest_dp = float(cart["dp"].iloc[-1])
        st.write(f"Latest DP: **{latest_dp:.2f} bar**")

        render_dp_gauge(latest_dp)

        if latest_dp < 1:
            st.success("DP Normal (< 1 bar)")
        elif latest_dp <= 4:
            st.warning("DP Warning (1–4 bar) • Monitor cartridge")
        else:
            st.error("DP High (> 4 bar) • Replacement required")

    # ----------------------------------------------------------
    # ❇ CHEMICAL STOCK
    # ----------------------------------------------------------

        # ----------------------------------------------------------
    # ❇ CHEMICAL STOCK
    # ----------------------------------------------------------

    section_title("Chemical Stock Levels")

    if len(chem) == 0:
        st.info("No chemicals found.")
    else:
        df_disp = chem.copy()
        df_disp["stock_value"] = df_disp["qty"] * df_disp["unit_cost"]
        st.dataframe(df_disp, use_container_width=True)

        for _, row in chem.iterrows():
            nm = row["name"]
            qty = float(row["qty"])
            min_stock = MIN_STOCK.get(nm, 30)  # default 30 kg if not defined

            st.write(f"**{nm}: {qty:.1f} kg (Min stock {min_stock} kg)**")
            st.progress(min(qty / max(min_stock * 2, 1), 1.0))

            # Specific rule for chlorine
            if "Hypochlorite" in nm or "Chlorine" in nm:
                if qty < 50:
                    st.error(f"⚠ Chlorine stock {qty:.1f} kg < 50 kg – URGENT reorder.")
                elif qty < 80:
                    st.warning(f"Chlorine stock {qty:.1f} kg – approaching low level.")
                continue

            # Generic rules for other chemicals
            if qty < min_stock:
                st.error(f"{nm} below minimum stock ({qty:.1f} kg < {min_stock} kg).")
            elif qty < min_stock * 1.3:
                st.warning(f"{nm} approaching minimum stock.")
# ===================

def compute_compliance(df):
    """
    Compute water quality compliance based on TDS and pH.
    TDS <= 50 ppm AND 6.5 <= pH <= 8.5 = in-spec.
    Returns: (compliance%, good_days, bad_days)
    """
    if len(df) == 0:
        return 0.0, 0, 0

    cond_ok = (df["tds"] <= 50) & (df["ph"].between(6.5, 8.5))
    total_days = len(df)
    good_days = int(cond_ok.sum())
    bad_days = total_days - good_days
    compliance = (good_days / total_days) * 100 if total_days else 0
    return round(compliance, 1), good_days, bad_days


# ============================================================
# 9) PAGE – ADD DAILY READING (EMERALD UI)
# ============================================================

def page_add_reading():
    st.markdown("<h1 style='color:#0E6655;'>Daily RO Reading – Um Qasr</h1>", unsafe_allow_html=True)
    st.caption("Record water quality, production, and maintenance for the RO unit.")

    with st.form("daily_reading_form"):
        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=date.today())
        with c2:
            tds = st.number_input("TDS (ppm)", min_value=0.0, step=0.1)
        with c3:
            ph = st.number_input("pH", min_value=0.0, max_value=14.0, step=0.01)

        c4, c5, c6 = st.columns(3)
        with c4:
            conductivity = st.number_input("Conductivity (µS/cm)", min_value=0.0, step=1.0)
        with c5:
            flow_m3 = st.number_input("Flow (m³/hr)", min_value=0.0, step=0.1)
        with c6:
            production = st.number_input("Production Today (m³)", min_value=0.0, step=0.1)

        st.markdown("<div class='sub-header'>Maintenance / Notes</div>", unsafe_allow_html=True)
        maintenance = st.text_area("Maintenance done today", height=80)
        notes = st.text_area("Notes / alarms / comments", height=80)

        submitted = st.form_submit_button("💾 Save Daily Reading")
        if submitted:
            add_reading(str(d), tds, ph, conductivity, flow_m3, production, maintenance, notes)
            st.success("✅ Daily RO reading saved successfully.")

    st.markdown("---")
    section_title("Last 10 Readings")

    df = get_readings()
    if len(df) > 0:
        st.dataframe(df.tail(10), use_container_width=True)
    else:
        st.info("No readings recorded yet.")


# ============================================================
# 10) PAGE – CHEMICALS (STOCK, COST & MOVEMENTS)
# ============================================================

def page_chemicals():
    st.markdown("<h1 style='color:#0E6655;'>Chemicals – Stock, Cost & Movements</h1>", unsafe_allow_html=True)
    st.caption("Track chemical stock, update unit costs, and log IN / OUT movements.")

    chem_df = get_chemicals()
    mov_df = get_chemical_movements()

    tab1, tab2, tab3 = st.tabs(["📦 Stock & Cost", "➕ Record IN / OUT", "📜 Movements History"])

    # ---------- TAB 1: STOCK & COST ----------
    with tab1:
        section_title("Current Stock and Value")

        if len(chem_df) == 0:
            st.info("No chemicals found in database.")
        else:
            # Ensure numeric types for qty and unit_cost
            chem_df = chem_df.copy()
            chem_df["qty"] = pd.to_numeric(chem_df["qty"], errors="coerce").fillna(0)
            chem_df["unit_cost"] = pd.to_numeric(chem_df["unit_cost"], errors="coerce").fillna(0)

            disp = chem_df.copy()
            disp["stock_value"] = disp["qty"] * disp["unit_cost"]

            st.dataframe(disp, use_container_width=True)

            st.markdown("<div class='sub-header'>Update Unit Cost (per kg)</div>", unsafe_allow_html=True)

            c1, c2, c3 = st.columns([1, 1, 0.6])
            with c1:
                chem_name = st.selectbox(
                    "Chemical",
                    chem_df["name"].tolist(),
                    key="chem_cost_name"
                )
            with c2:
                current_cost = float(
                    chem_df.loc[chem_df["name"] == chem_name, "unit_cost"].iloc[0]
                )
                new_cost = st.number_input(
                    "Unit Cost (per kg)",
                    min_value=0.0,
                    step=0.1,
                    value=current_cost,
                    key="chem_unit_cost"
                )
            with c3:
                if st.button("💾 Save Cost", key="save_cost_btn"):
                    update_chemical_cost(chem_name, new_cost)
                    st.success(f"✅ Unit cost updated for {chem_name}.")

    # ---------- TAB 2: RECORD MOVEMENTS ----------
    with tab2:
        section_title("Record Chemical IN / OUT")

        c1, c2, c3 = st.columns(3)
        with c1:
            d = st.date_input("Date", value=date.today(), key="chem_date")
        with c2:
            if len(chem_df) > 0:
                name = st.selectbox(
                    "Chemical",
                    chem_df["name"].tolist(),
                    key="chem_name_select"
                )
            else:
                name = st.text_input("Chemical Name", key="chem_name_manual")
        with c3:
            movement_type = st.selectbox("Movement Type", ["IN", "OUT"], key="chem_move_type")

        qty = st.number_input("Quantity (kg)", min_value=0.0, step=0.1, key="chem_qty")
        remarks = st.text_input(
            "Remarks / reference (invoice, batch, etc.)",
            key="chem_remarks"
        )

        if st.button("💾 Save Movement", key="chem_move_save"):
            if not name:
                st.error("Please select or enter a chemical name.")
            elif qty <= 0:
                st.error("Quantity must be greater than 0.")
            else:
                record_chemical_movement(str(d), name, movement_type, qty, remarks)
                st.success(f"✅ {movement_type} movement recorded for {name}.")

    # ---------- TAB 3: HISTORY ----------
    with tab3:
        section_title("All Chemical Movements")

        if len(mov_df) == 0:
            st.info("No chemical movements recorded yet.")
        else:
            st.dataframe(mov_df, use_container_width=True)


# ============================================================
# 11) PAGE – CARTRIDGE FILTER (DP MONITORING)
# ============================================================

def page_cartridge():
    st.markdown("<h1 style='color:#0E6655;'>Cartridge Filter – DP Monitoring</h1>", unsafe_allow_html=True)
    st.caption("Monitor differential pressure (DP) and log cartridge actions.")

    st.markdown("<div class='sub-header'>Enter Pressure Readings</div>", unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        before = st.number_input(
            "Pressure BEFORE Filter (bar)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,
            step=0.1
        )
    with col2:
        after = st.number_input(
            "Pressure AFTER Filter (bar)",
            min_value=0.0,
            max_value=10.0,
            value=0.0,
            step=0.1
        )

    if after > before:
        st.warning("After-pressure cannot be higher than before. Adjusting AFTER to BEFORE.")
        after = before

    dp = before - after
    if dp < 0:
        dp = 0.0

    kpi_card("Differential Pressure (DP)", f"{dp:.2f} bar")

    st.markdown("<div class='sub-header'>DP Gauge (0–10 bar)</div>", unsafe_allow_html=True)
    render_dp_gauge(dp)

    if dp < 1:
        st.success("DP Normal – Cartridge OK (DP < 1 bar).")
    elif dp <= 4:
        st.warning("DP Warning (1–4 bar) – Monitor filter.")
    else:
        st.error("High DP > 4 bar – Replace cartridge immediately.")

    st.markdown("---")
    section_title("Save Cartridge Record")

    c1, c2 = st.columns(2)
    with c1:
        d = st.date_input("Date", value=date.today(), key="cart_date")
    with c2:
        is_change = st.checkbox("Cartridge replaced during this visit?", key="cart_is_change")

    remarks = st.text_input(
        "Remarks (change / inspection / cleaning)",
        key="cart_remarks"
    )

    change_cost = 0.0
    if is_change:
        change_cost = st.number_input(
            "Replacement Cost",
            min_value=0.0,
            step=1.0,
            value=0.0,
            key="cart_cost"
        )

    if st.button("💾 Save Cartridge Log", key="cart_save_btn"):
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
# 12) PDF REPORT HELPERS (MONTHLY & MAINTENANCE)
# ============================================================

def create_pdf(month, df_month, df_chem_out, df_cart):
    filename = f"RO_Report_{month}.pdf"
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)

    pdf.cell(0, 10, f"Monthly RO Report - Um Qasr Port - {month}", ln=True)
    pdf.ln(4)

    # ---------------- Water KPIs ----------------
    pdf.set_font("Arial", "", 11)
    avg_tds = df_month["tds"].mean() if len(df_month) > 0 else 0.0
    avg_ph = df_month["ph"].mean() if len(df_month) > 0 else 0.0
    out_days = ((df_month["tds"] > 50) | (~df_month["ph"].between(6.5, 8.5))).sum() if len(df_month) > 0 else 0
    total_prod = df_month["production"].sum() if len(df_month) > 0 else 0.0

    pdf.multi_cell(
        0,
        7,
        (
            f"Average TDS: {avg_tds:.2f}\n"
            f"Average pH: {avg_ph:.2f}\n"
            f"Out-of-spec Days: {int(out_days)}\n"
            f"Total Production (m3): {total_prod:.1f}\n"
        ),
    )

    # ---------------- Chemical Cost Summary ----------------
    pdf.ln(3)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Chemical Usage and Costs (kg):", ln=True)
    pdf.set_font("Arial", "", 11)

    chem_df = get_chemicals()
    cost_map = {row["name"]: float(row["unit_cost"]) for _, row in chem_df.iterrows()} if len(chem_df) > 0 else {}

    total_chem_cost = 0.0

    if len(df_chem_out) > 0:
        usage = df_chem_out.groupby("name")["qty"].sum().reset_index()

        for _, r in usage.iterrows():
            ch = r["name"]
            qty = float(r["qty"])
            unit_cost = cost_map.get(ch, 0.0)
            chem_cost = qty * unit_cost
            total_chem_cost += chem_cost
            rate_kg_m3 = qty / total_prod if total_prod > 0 else 0.0
            rate_cost_m3 = chem_cost / total_prod if total_prod > 0 else 0.0

            pdf.multi_cell(
                0,
                6,
                f"{ch}: qty={qty:.2f} kg, cost={chem_cost:.2f}, rate={rate_kg_m3:.4f} kg/m3, cost_rate={rate_cost_m3:.4f}/m3",
            )
    else:
        pdf.cell(0, 6, "No chemical consumption recorded this month.", ln=True)

    # ---------------- Cartridge Filter Cost ----------------
    pdf.ln(4)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Cartridge Filter Activity:", ln=True)
    pdf.set_font("Arial", "", 11)

    total_cart_cost = 0.0
    num_changes = 0

    if len(df_cart) > 0:
        df_cart = df_cart.copy()
        if "is_change" not in df_cart.columns:
            df_cart["is_change"] = 0
        if "change_cost" not in df_cart.columns:
            df_cart["change_cost"] = 0.0

        num_changes = int(df_cart["is_change"].fillna(0).sum())
        total_cart_cost = float(df_cart["change_cost"].fillna(0.0).sum())

        for _, r in df_cart.iterrows():
            d_str = r["d"].date().isoformat()
            dp = float(r["dp"])
            remarks = str(r["remarks"] or "")
            is_change = int(r["is_change"])
            c_cost = float(r["change_cost"])
            tag = "CHANGE" if is_change else "CHECK"

            pdf.cell(
                0,
                6,
                f"{d_str}: DP={dp:.2f} bar - {tag} - cost={c_cost:.2f} - {remarks}",
                ln=True,
            )
    else:
        pdf.cell(0, 6, "No cartridge activity recorded this month.", ln=True)

    # ---------------- Total Cost / m3 ----------------
    pdf.ln(4)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "Total Consumable Cost:", ln=True)
    pdf.set_font("Arial", "", 11)

    total_cost = total_chem_cost + total_cart_cost
    cost_rate = total_cost / total_prod if total_prod > 0 else 0.0

    pdf.multi_cell(
        0,
        6,
        (
            f"Total chemical cost: {total_chem_cost:.2f}\n"
            f"Total cartridge cost: {total_cart_cost:.2f}\n"
            f"Overall consumable cost: {total_cost:.2f}\n"
            f"Cost per m3: {cost_rate:.4f}/m3\n"
        ),
    )

    pdf.output(filename)
    return filename


def create_maintenance_pdf(month, df_maint, df_cart):
    filename = f"RO_Maintenance_{month}.pdf"
    pdf = FPDF()
    pdf.add_page()
    pdf.set_font("Arial", "B", 14)

    pdf.cell(0, 10, f"Maintenance Report - Um Qasr RO - {month}", ln=True)
    pdf.ln(4)

    pdf.set_font("Arial", "", 11)
    pdf.multi_cell(
        0,
        7,
        (
            "This report contains maintenance actions, cartridge changes, DP checks, "
            "notes, and consumable costs for the selected month.\n"
        ),
    )

    # ---- Daily Maintenance ----
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "1) Daily Maintenance Logs", ln=True)
    pdf.set_font("Arial", "", 11)

    if len(df_maint) > 0:
        for _, r in df_maint.iterrows():
            date_str = r["d"].date().isoformat()
            mtxt = (r["maintenance"] or "").strip()
            notes = (r["notes"] or "").strip()
            pdf.multi_cell(0, 6, f"- {date_str}: {mtxt} (Notes: {notes})")
    else:
        pdf.cell(0, 6, "No maintenance logs for this month.", ln=True)

    # ---- Cartridge Section ----
    pdf.ln(4)
    pdf.set_font("Arial", "B", 12)
    pdf.cell(0, 8, "2) Cartridge Filter Actions", ln=True)
    pdf.set_font("Arial", "", 11)

    if len(df_cart) > 0:
        for _, r in df_cart.iterrows():
            d_str = r["d"].date().isoformat()
            dp = float(r["dp"])
            remarks = r["remarks"]
            is_change = int(r["is_change"])
            cost = float(r["change_cost"])
            tag = "CHANGE" if is_change else "CHECK"
            pdf.cell(
                0,
                6,
                f"- {d_str}: DP={dp:.2f} bar - {tag} - cost={cost:.2f} - {remarks}",
                ln=True,
            )
    else:
        pdf.cell(0, 6, "No cartridge entries for this month.", ln=True)

    pdf.output(filename)
    return filename


# ============================================================
# 13) PAGE – MONTHLY REPORT (WATER, CHEMICALS & COST)
# ============================================================

def page_monthly_report():
    st.markdown("<h1 style='color:#0E6655;'>Monthly Report – Water, Chemicals & Cost</h1>", unsafe_allow_html=True)
    st.caption("Analyze monthly performance, chemical costs, and cartridge activity.")

    df = get_readings()
    if len(df) == 0:
        st.info("No readings data yet.")
        return

    df["month"] = df["d"].dt.strftime("%Y-%m")
    months = sorted(df["month"].unique())
    month = st.selectbox("Select Month (YYYY-MM)", months)

    df_m = df[df["month"] == month]
    total_prod = df_m["production"].sum() if len(df_m) > 0 else 0.0

    # Chemical OUT (consumption) for that month
    chem_mov = get_chemical_movements()
    if len(chem_mov) > 0:
        chem_mov["month"] = chem_mov["d"].dt.strftime("%Y-%m")
        df_chem_out = chem_mov[
            (chem_mov["month"] == month) &
            (chem_mov["movement_type"] == "OUT")
        ]
    else:
        df_chem_out = pd.DataFrame(columns=["d", "name", "movement_type", "qty", "remarks"])

    # Cartridge records that month
    cart_df = get_cartridge()
    if len(cart_df) > 0:
        cart_df["month"] = cart_df["d"].dt.strftime("%Y-%m")
        df_cart_m = cart_df[cart_df["month"] == month]
    else:
        df_cart_m = pd.DataFrame(columns=["d", "dp", "remarks", "is_change", "change_cost"])

    # ---- Water trend ----
    section_title("Water Quality Trend (TDS / pH / Conductivity)")
    if len(df_m) > 0:
        fig, ax = plt.subplots(figsize=(6, 3))
        ax.plot(df_m["d"], df_m["tds"], label="TDS", color=EMERALD_GREEN)
        ax.plot(df_m["d"], df_m["ph"], label="pH", color=DARK_GREEN)
        ax.plot(df_m["d"], df_m["conductivity"], label="Conductivity", color="#117A65")
        ax.legend()
        ax.grid(alpha=0.3)
        plt.xticks(rotation=45)
        st.pyplot(fig)
    else:
        st.info("No daily data for this month.")

    # ---- Detail table ----
    section_title("Daily Details")
    st.dataframe(
        df_m[
            [
                "d",
                "tds",
                "ph",
                "conductivity",
                "flow_m3",
                "production",
                "maintenance",
                "notes",
            ]
        ],
        use_container_width=True
    )

    # ---- Chemical consumption summary ----
    section_title("Chemical Consumption (OUT) – This Month")
    if len(df_chem_out) > 0:
        st.dataframe(df_chem_out, use_container_width=True)

        chem_df = get_chemicals()
        cost_map = {row["name"]: float(row["unit_cost"]) for _, row in chem_df.iterrows()} if len(chem_df) > 0 else {}

        usage = df_chem_out.groupby("name")["qty"].sum().reset_index()
        rows = []
        total_chem_cost = 0.0

        for _, r in usage.iterrows():
            ch = r["name"]
            qty = float(r["qty"])
            unit_cost = cost_map.get(ch, 0.0)
            chem_cost = qty * unit_cost
            total_chem_cost += chem_cost
            rate_kg_m3 = qty / total_prod if total_prod > 0 else 0.0
            rate_cost_m3 = chem_cost / total_prod if total_prod > 0 else 0.0
            rows.append(
                {
                    "Chemical": ch,
                    "Qty (kg)": qty,
                    "Unit cost": unit_cost,
                    "Total cost": chem_cost,
                    "Rate (kg/m3)": rate_kg_m3,
                    "Cost rate (/m3)": rate_cost_m3,
                }
            )

        summary_df = pd.DataFrame(rows)
        st.markdown("**Chemical usage and cost summary:**")
        st.dataframe(summary_df, use_container_width=True)
    else:
        st.info("No chemical OUT movements recorded this month.")
        total_chem_cost = 0.0

    # ---- Cartridge activity & cost ----
    section_title("Cartridge Filter Activity – This Month")
    if len(df_cart_m) > 0:
        st.dataframe(df_cart_m, use_container_width=True)

        if "is_change" not in df_cart_m.columns:
            df_cart_m["is_change"] = 0
        if "change_cost" not in df_cart_m.columns:
            df_cart_m["change_cost"] = 0.0

        num_changes = int(df_cart_m["is_change"].fillna(0).sum())
        total_cart_cost = float(df_cart_m["change_cost"].fillna(0.0).sum())
    else:
        st.info("No cartridge activity this month.")
        num_changes = 0
        total_cart_cost = 0.0

    # ---- Global consumables cost ----
    section_title("Monthly Consumables Cost Summary")
    total_consumable_cost = total_chem_cost + total_cart_cost
    cost_per_m3 = total_consumable_cost / total_prod if total_prod > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        kpi_card("Total Chemical Cost", f"{total_chem_cost:.2f}")
    with col2:
        kpi_card("Cartridge Changes", f"{num_changes}")
    with col3:
        kpi_card("Cartridge Cost", f"{total_cart_cost:.2f}")
    with col4:
        kpi_card("Total Cost per m³", f"{cost_per_m3:.4f}")

    # ---- PDF Export ----
    st.markdown("---")
    if st.button("📄 Export Monthly Water/Chem PDF"):
        file = create_pdf(month, df_m, df_chem_out, df_cart_m)
        with open(file, "rb") as f:
            st.download_button("⬇ Download Monthly PDF", f, file_name=file, mime="application/pdf")
        st.success("Monthly PDF report generated.")


# ============================================================
# 14) PAGE – MAINTENANCE REPORT
# ============================================================

def page_maintenance_report():
    st.markdown("<h1 style='color:#0E6655;'>Maintenance Report – Daily Actions & Cartridge</h1>", unsafe_allow_html=True)
    st.caption("Generate a monthly maintenance and cartridge activity report.")

    df = get_readings()
    if len(df) == 0:
        st.info("No readings data yet.")
        return

    df["month"] = df["d"].dt.strftime("%Y-%m")
    months = sorted(df["month"].unique())
    month = st.selectbox("Select Month (YYYY-MM)", months)

    df_m = df[df["month"] == month]

    # Daily maintenance rows with text
    if len(df_m) > 0:
        df_maint = df_m.copy()
        df_maint["maintenance"] = df_maint["maintenance"].fillna("")
        df_maint["notes"] = df_maint["notes"].fillna("")
        df_maint = df_maint[df_maint["maintenance"].str.strip() != ""]
    else:
        df_maint = pd.DataFrame(columns=df.columns)

    # Cartridge records for that month
    cart_df = get_cartridge()
    if len(cart_df) > 0:
        cart_df["month"] = cart_df["d"].dt.strftime("%Y-%m")
        df_cart_m = cart_df[cart_df["month"] == month]
    else:
        df_cart_m = pd.DataFrame(columns=["d", "dp", "remarks", "is_change", "change_cost"])

    section_title("Daily Maintenance Notes")
    if len(df_maint) > 0:
        st.dataframe(df_maint[["d", "maintenance", "notes"]], use_container_width=True)
    else:
        st.info("No maintenance notes recorded for this month.")

    section_title("Cartridge Filter DP & Actions")
    if len(df_cart_m) > 0:
        st.dataframe(df_cart_m, use_container_width=True)
    else:
        st.info("No cartridge entries for this month.")

    st.markdown("---")
    if st.button("📄 Export Maintenance PDF"):
        file = create_maintenance_pdf(month, df_maint, df_cart_m)
        with open(file, "rb") as f:
            st.download_button("⬇ Download Maintenance PDF", f, file_name=file, mime="application/pdf")
        st.success("Maintenance PDF generated.")


# ============================================================
# 15) MAIN ROUTER
# ============================================================

def main():
    # Sidebar branding
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
