# Um Qasr RO System - Single File App
# Global RO Dashboard with Neon PostgreSQL backend

import streamlit as st
import pandas as pd
import datetime
from io import BytesIO

import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# CONFIG
# =========================

DB_URL = "postgresql://neondb_owner:npg_C4ghxK1yUcfw@ep-billowing-fog-agxbr2fc-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"


# =========================
# DB HELPERS
# =========================

def get_conn():
    return psycopg2.connect(DB_URL)


def init_db():
    """Create tables if they do not exist."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS flowmeter_readings (
            id SERIAL PRIMARY KEY,
            reading_date DATE NOT NULL UNIQUE,
            reading_value NUMERIC(12,2) NOT NULL,
            operator VARCHAR(100),
            notes TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS daily_production (
            id SERIAL PRIMARY KEY,
            prod_date DATE NOT NULL UNIQUE,
            prod_value NUMERIC(12,2) NOT NULL,
            cumulative_month NUMERIC(12,2),
            cumulative_total NUMERIC(12,2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS cartridge_filters (
            id SERIAL PRIMARY KEY,
            entry_date DATE NOT NULL,
            pressure_before NUMERIC(6,2),
            pressure_after NUMERIC(6,2),
            diff_pressure NUMERIC(6,2),
            status VARCHAR(20),
            operator VARCHAR(100),
            notes TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS chemicals_movement (
            id SERIAL PRIMARY KEY,
            movement_date DATE NOT NULL,
            chemical VARCHAR(50) NOT NULL,
            qty_in NUMERIC(12,2),
            qty_out NUMERIC(12,2),
            balance NUMERIC(12,2),
            operator VARCHAR(100),
            notes TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS chemicals_stock (
            chemical VARCHAR(50) PRIMARY KEY,
            stock_qty NUMERIC(12,2)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS system_status (
            id SERIAL PRIMARY KEY,
            status_time TIMESTAMP DEFAULT NOW(),
            hp_pump BOOLEAN,
            lp_pump BOOLEAN,
            feed_pump BOOLEAN,
            ro_running BOOLEAN
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS maintenance_log (
            id SERIAL PRIMARY KEY,
            maint_date DATE,
            component VARCHAR(100),
            action TEXT,
            operator VARCHAR(100),
            notes TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS operators (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            role VARCHAR(50)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS water_quality (
            id SERIAL PRIMARY KEY,
            sample_date DATE NOT NULL,
            sample_time TIME,
            point VARCHAR(50),
            tds NUMERIC(10,2),
            ph NUMERIC(4,2),
            conductivity NUMERIC(10,2),
            turbidity NUMERIC(10,2),
            operator VARCHAR(100),
            notes TEXT
        );
        """
    ]
    conn = get_conn()
    cur = conn.cursor()
    for ddl in ddl_statements:
        cur.execute(ddl)
    conn.commit()
    cur.close()
    conn.close()


def run_query(sql, params=None, fetch=False):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, params or [])
    data = None
    if fetch:
        data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return data


def fetch_df(sql, params=None):
    conn = get_conn()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


# =========================
# EXPORT HELPERS
# =========================

try:
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.pdfgen import canvas
    REPORTLAB_AVAILABLE = True
except Exception:
    REPORTLAB_AVAILABLE = False


def export_df_to_excel(df: pd.DataFrame, sheet_name: str = "Sheet1") -> BytesIO:
    """Return an in-memory Excel file from a DataFrame."""
    from pandas import ExcelWriter
    buffer = BytesIO()
    with ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    buffer.seek(0)
    return buffer


def export_simple_pdf(title: str, lines: list) -> BytesIO:
    """Create a basic PDF (landscape A4) with title and text lines."""
    buffer = BytesIO()
    if not REPORTLAB_AVAILABLE:
        buffer.write(b"")
        buffer.seek(0)
        return buffer

    c = canvas.Canvas(buffer, pagesize=landscape(A4))
    width, height = landscape(A4)
    y = height - 40

    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, y, title)
    y -= 40

    c.setFont("Helvetica", 10)
    for line in lines:
        if y < 40:
            c.showPage()
            y = height - 40
            c.setFont("Helvetica", 10)
        c.drawString(40, y, str(line))
        y -= 14

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer


# =========================
# THEME / STYLING
# =========================

THEME_CSS = """
<style>
/* Global dark blue gradient SCADA theme */
body {
    background: radial-gradient(circle at top left, #021b3a, #000814 55%, #001233 100%);
    color: #f4f4f4;
    font-family: "Segoe UI", system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
}

.block-container {
    padding-top: 1.5rem;
    padding-bottom: 2rem;
    max-width: 1300px;
}

/* Titles */
.top-title {
    font-size: 2rem;
    font-weight: 700;
    color: #e0f4ff;
    text-shadow: 0 0 12px rgba(0, 204, 255, 0.6);
    margin-bottom: 0.2rem;
}
.subtitle {
    font-size: 0.95rem;
    color: #8fb9ff;
    margin-bottom: 1.5rem;
}

/* KPI cards */
.kpi-card {
    background: linear-gradient(135deg, rgba(0, 180, 255, 0.15), rgba(0, 0, 0, 0.65));
    border-radius: 12px;
    padding: 0.9rem 1rem;
    border: 1px solid rgba(0, 180, 255, 0.4);
    box-shadow: 0 0 18px rgba(0, 128, 255, 0.35);
}
.kpi-label {
    font-size: 0.8rem;
    color: #9ac9ff;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.kpi-value {
    font-size: 1.4rem;
    font-weight: 700;
    color: #e8f7ff;
}

/* Status pills */
.status-pill {
    display: inline-block;
    padding: 0.2rem 0.65rem;
    border-radius: 999px;
    font-size: 0.75rem;
    font-weight: 600;
}
.status-ok {
    background: rgba(0, 200, 83, 0.15);
    color: #00e676;
    border: 1px solid rgba(0, 200, 83, 0.5);
}
.status-warn {
    background: rgba(255, 196, 0, 0.2);
    color: #ffeb3b;
    border: 1px solid rgba(255, 214, 0, 0.7);
}
.status-alarm {
    background: rgba(244, 67, 54, 0.2);
    color: #ff5252;
    border: 1px solid rgba(244, 67, 54, 0.7);
}

/* Sidebar */
section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #001845, #000814);
}
section[data-testid="stSidebar"] * {
    color: #e0f4ff;
}

/* Tables */
.dataframe tbody tr:nth-child(odd) {
    background-color: rgba(3, 37, 76, 0.7);
}
.dataframe tbody tr:nth-child(even) {
    background-color: rgba(0, 18, 51, 0.7);
}
.dataframe thead {
    background-color: rgba(0, 119, 182, 0.85);
    color: #ffffff;
}

/* Buttons */
.stButton>button, .stDownloadButton>button {
    border-radius: 999px;
    padding: 0.4rem 1.1rem;
    border: 1px solid rgba(0, 212, 255, 0.7);
    background: linear-gradient(135deg, #0099ff, #00d4ff);
    color: #00111f;
    font-weight: 600;
}
.stButton>button:hover, .stDownloadButton>button:hover {
    filter: brightness(1.05);
    box-shadow: 0 0 12px rgba(0, 212, 255, 0.7);
}
</style>
"""


def apply_theme():
    st.markdown(THEME_CSS, unsafe_allow_html=True)
# =========================
# PAGES
# =========================

def page_dashboard():
    apply_theme()
    st.markdown("<div class='top-title'>RO Plant – Production & Health Dashboard</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Today KPIs, monthly performance, water quality and system status</div>",
                unsafe_allow_html=True)

    today = datetime.date.today()
    first_month = today.replace(day=1)

    # Production this month
    df_prod = fetch_df(
        "SELECT * FROM daily_production WHERE prod_date >= %s AND prod_date <= %s ORDER BY prod_date",
        (first_month, today)
    )

    if df_prod.empty:
        today_val = 0.0
        month_total = 0.0
    else:
        df_prod["prod_date"] = pd.to_datetime(df_prod["prod_date"])
        today_val = float(df_prod[df_prod["prod_date"].dt.date == today]["prod_value"].sum())
        month_total = float(df_prod["prod_value"].sum())

    df_life = fetch_df("SELECT COALESCE(SUM(prod_value),0) AS tot FROM daily_production")
    lifetime = float(df_life["tot"].iloc[0]) if not df_life.empty else 0.0

    # Cartridge latest
    df_cart = fetch_df("SELECT * FROM cartridge_filters ORDER BY entry_date DESC, id DESC LIMIT 1")
    cart_status = "No data"
    cart_class = ""
    diff_val = None
    if not df_cart.empty:
        diff_val = float(df_cart["diff_pressure"].iloc[0] or 0)
        if diff_val < 1:
            cart_status = "OK (<1 bar)"
            cart_class = "status-ok"
        elif diff_val < 2:
            cart_status = "Warning (1–2 bar)"
            cart_class = "status-warn"
        else:
            cart_status = "ALARM (>2 bar)"
            cart_class = "status-alarm"

    # Chemical stock
    df_stock = fetch_df("SELECT * FROM chemicals_stock ORDER BY chemical")
    low_chems = []
    for _, row in df_stock.iterrows():
        q = float(row["stock_qty"] or 0)
        lvl = None
        if q <= 0:
            lvl = "Empty"
        elif q < 50:
            lvl = "Critical <50"
        elif q < 100:
            lvl = "Low <100"
        if lvl:
            low_chems.append(f"{row['chemical']}: {lvl} (current {q:.1f})")

    # Latest permeate water quality
    df_wq = fetch_df(
        "SELECT * FROM water_quality WHERE point=%s ORDER BY sample_date DESC, sample_time DESC, id DESC LIMIT 1",
        ("Permeate",)
    )
    last_tds = None
    last_ph = None
    if not df_wq.empty:
        last_tds = float(df_wq["tds"].iloc[0] or 0)
        last_ph = float(df_wq["ph"].iloc[0] or 0)

    # KPI cards
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        st.markdown(
            f"<div class='kpi-card'><div class='kpi-label'>Today Production</div>"
            f"<div class='kpi-value'>{today_val:,.1f} m³</div></div>",
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            f"<div class='kpi-card'><div class='kpi-label'>Month-to-Date</div>"
            f"<div class='kpi-value'>{month_total:,.1f} m³</div></div>",
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            f"<div class='kpi-card'><div class='kpi-label'>Lifetime Production</div>"
            f"<div class='kpi-value'>{lifetime:,.1f} m³</div></div>",
            unsafe_allow_html=True,
        )
    with c4:
        if cart_status != "No data":
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Cartridge Status</div>"
                f"<div class='kpi-value'><span class='status-pill {cart_class}'>{cart_status}</span></div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Cartridge Status</div>"
                "<div class='kpi-value'>No data</div></div>",
                unsafe_allow_html=True,
            )
    with c5:
        if last_tds is not None:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Permeate TDS</div>"
                f"<div class='kpi-value'>{last_tds:,.0f} ppm</div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Permeate TDS</div>"
                "<div class='kpi-value'>No data</div></div>",
                unsafe_allow_html=True,
            )
    with c6:
        if last_ph is not None:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Permeate pH</div>"
                f"<div class='kpi-value'>{last_ph:.2f}</div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Permeate pH</div>"
                "<div class='kpi-value'>No data</div></div>",
                unsafe_allow_html=True,
            )

    st.markdown("---")

    col_chart, col_alerts = st.columns([2, 1])

    with col_chart:
        st.subheader("Daily Production – This Month")
        if df_prod.empty:
            st.info("No production data for this month yet. Add flowmeter readings and recalculate.")
        else:
            df_plot = df_prod.sort_values("prod_date")
            st.bar_chart(df_plot.set_index("prod_date")["prod_value"])

    with col_alerts:
        st.subheader("Alerts & Warnings")
        if low_chems:
            st.warning("Chemical stock alerts:")
            for msg in low_chems:
                st.write("• " + msg)
        else:
            st.success("No chemical stock alerts.")

        if diff_val is not None and diff_val >= 2:
            st.error(f"Cartridge filter ΔP high: {diff_val:.2f} bar – change filter.")
        elif diff_val is not None and diff_val >= 1:
            st.warning(f"Cartridge filter ΔP elevated: {diff_val:.2f} bar – monitor.")
def page_flowmeter():
    apply_theme()
    st.markdown("<div class='top-title'>Flowmeter Readings</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Daily raw readings from RO product flowmeter</div>", unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Add / Update Reading")
        date_val = st.date_input("Reading Date", datetime.date.today())
        reading = st.number_input("Flowmeter Reading (totalizer m³)", min_value=0.0, step=0.1)
        operator = st.text_input("Operator", "")
        notes = st.text_area("Notes", "")

        if st.button("💾 Save Reading"):
            run_query(
                """
                INSERT INTO flowmeter_readings (reading_date, reading_value, operator, notes)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (reading_date)
                DO UPDATE SET reading_value = EXCLUDED.reading_value,
                              operator = EXCLUDED.operator,
                              notes = EXCLUDED.notes;
                """,
                (date_val, reading, operator or None, notes or None),
                fetch=False,
            )
            st.success("Reading saved / updated.")

    with col_table:
        st.subheader("History")
        days_back = st.slider("Show last N days", 7, 120, 30)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT * FROM flowmeter_readings WHERE reading_date >= %s ORDER BY reading_date DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No readings found for selected period.")
        else:
            st.dataframe(df)

    st.markdown("---")
    st.subheader("Generate Daily Production from Flowmeter")

    if st.button("⚙️ Recalculate Daily Production from all readings"):
        df_all = fetch_df("SELECT * FROM flowmeter_readings ORDER BY reading_date")
        if df_all.shape[0] < 2:
            st.warning("Need at least 2 readings to calculate daily production.")
        else:
            # Clear old data
            run_query("DELETE FROM daily_production;", fetch=False)

            last_val = None
            current_month = None
            cumulative_month = 0.0
            cumulative_total = 0.0

            for _, row in df_all.iterrows():
                d = row["reading_date"]
                val = float(row["reading_value"])
                if last_val is None:
                    prod = 0.0
                else:
                    prod = max(val - last_val, 0.0)
                last_val = val

                # month handling
                if current_month is None or d.month != current_month.month or d.year != current_month.year:
                    cumulative_month = prod
                    current_month = d
                else:
                    cumulative_month += prod
                cumulative_total += prod

                run_query(
                    "INSERT INTO daily_production (prod_date, prod_value, cumulative_month, cumulative_total) "
                    "VALUES (%s,%s,%s,%s)",
                    (d, prod, cumulative_month, cumulative_total),
                    fetch=False,
                )
            st.success("Daily production recalculated from flowmeter readings.")


def page_production():
    apply_theme()
    st.markdown("<div class='top-title'>Production Reports</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Daily, monthly and cumulative RO production</div>", unsafe_allow_html=True)

    today = datetime.date.today()
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("From date", today.replace(day=1))
    with col2:
        end_date = st.date_input("To date", today)

    df = fetch_df(
        "SELECT * FROM daily_production WHERE prod_date >= %s AND prod_date <= %s ORDER BY prod_date",
        (start_date, end_date),
    )

    if df.empty:
        st.info("No production records for selected period.")
        return

    df["prod_date"] = pd.to_datetime(df["prod_date"])

    st.subheader("Production Table")
    st.dataframe(df)

    st.subheader("Charts")
    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("Daily Production (m³)")
        st.bar_chart(df.set_index("prod_date")["prod_value"])
    with col_b:
        st.caption("Cumulative Production (Total)")
        st.line_chart(df.set_index("prod_date")["cumulative_total"])

    st.markdown("---")
    st.subheader("Export")

    # Excel
    buf_x = export_df_to_excel(df, sheet_name="Production")
    st.download_button(
        "⬇️ Download production_report.xlsx",
        data=buf_x.getvalue(),
        file_name="production_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # PDF
    lines = [f"Production report from {start_date} to {end_date}", ""]
    for _, r in df.iterrows():
        lines.append(
            f"{r['prod_date'].date()}  -  {r['prod_value']} m³  (Cum: {r['cumulative_total']})"
        )
    buf_p = export_simple_pdf("RO Production Report", lines)
    st.download_button(
        "⬇️ Download production_report.pdf",
        data=buf_p.getvalue(),
        file_name="production_report.pdf",
        mime="application/pdf",
    )
def page_chemicals():
    apply_theme()
    st.markdown("<div class='top-title'>Chemical Movement IN / OUT</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Track antiscalant, chlorine, caustic, bisulfite stock and usage</div>",
                unsafe_allow_html=True)

    chemicals = ["Antiscalant", "Chlorine", "Caustic", "Sodium Bisulfite"]

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Record Movement")
        m_date = st.date_input("Date", datetime.date.today())
        chem = st.selectbox("Chemical", chemicals)
        qty_in = st.number_input("Qty IN", min_value=0.0, step=0.1)
        qty_out = st.number_input("Qty OUT", min_value=0.0, step=0.1)
        operator = st.text_input("Operator", "")
        notes = st.text_area("Notes", "")

        if st.button("💾 Save Movement"):
            # get last balance
            df_last = fetch_df(
                "SELECT balance FROM chemicals_movement WHERE chemical=%s "
                "ORDER BY movement_date DESC, id DESC LIMIT 1",
                (chem,),
            )
            last_bal = float(df_last["balance"].iloc[0]) if not df_last.empty else 0.0
            new_bal = last_bal + qty_in - qty_out

            run_query(
                "INSERT INTO chemicals_movement "
                "(movement_date, chemical, qty_in, qty_out, balance, operator, notes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (m_date, chem, qty_in or None, qty_out or None, new_bal, operator or None, notes or None),
                fetch=False,
            )
            # update stock
            run_query(
                """
                INSERT INTO chemicals_stock (chemical, stock_qty)
                VALUES (%s,%s)
                ON CONFLICT (chemical)
                DO UPDATE SET stock_qty = EXCLUDED.stock_qty;
                """,
                (chem, new_bal),
                fetch=False,
            )
            st.success(f"Movement saved. New balance for {chem}: {new_bal:.2f}")

    with col_table:
        st.subheader("Recent Movements")
        days_back = st.slider("Show last N days", 7, 120, 30)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT * FROM chemicals_movement WHERE movement_date >= %s "
            "ORDER BY movement_date DESC, id DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No chemical movements for selected period.")
        else:
            st.dataframe(df)

    st.markdown("---")
    st.subheader("Current Stock Levels")
    df_stock = fetch_df("SELECT * FROM chemicals_stock ORDER BY chemical")
    if df_stock.empty:
        st.info("No stock data yet. Save movements to generate stock.")
    else:
        st.dataframe(df_stock)


def page_filters():
    apply_theme()
    st.markdown("<div class='top-title'>Cartridge Filter Status</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Pressure before & after, differential and color-coded condition</div>",
                unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Log Filter Reading")
        d = st.date_input("Date", datetime.date.today())
        p_before = st.number_input("Pressure Before (bar)", min_value=0.0, step=0.1)
        p_after = st.number_input("Pressure After (bar)", min_value=0.0, step=0.1)
        operator = st.text_input("Operator", "")
        notes = st.text_area("Notes", "")

        diff = max(p_after - p_before, 0.0)
        if diff < 1:
            status = "OK"
            status_class = "status-ok"
            msg = "Filter clean."
        elif diff < 2:
            status = "Warning"
            status_class = "status-warn"
            msg = "Monitor filter, getting loaded."
        else:
            status = "Alarm"
            status_class = "status-alarm"
            msg = "Change filter – high differential."

        st.markdown(
            f"Current ΔP: **{diff:.2f} bar** – "
            f"<span class='status-pill {status_class}'>{status}</span>",
            unsafe_allow_html=True,
        )
        st.caption(msg)

        if st.button("💾 Save Reading"):
            run_query(
                "INSERT INTO cartridge_filters "
                "(entry_date, pressure_before, pressure_after, diff_pressure, status, operator, notes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (d, p_before, p_after, diff, status, operator or None, notes or None),
                fetch=False,
            )
            st.success("Cartridge filter reading saved.")

    with col_table:
        st.subheader("History")
        days_back = st.slider("Show last N days", 7, 120, 30)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT * FROM cartridge_filters WHERE entry_date >= %s "
            "ORDER BY entry_date DESC, id DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No cartridge filter logs for selected period.")
        else:
            st.dataframe(df)


def page_maintenance():
    apply_theme()
    st.markdown("<div class='top-title'>Maintenance Logs</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Record preventive and corrective maintenance for RO system</div>",
                unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Log Maintenance")
        d = st.date_input("Maintenance Date", datetime.date.today())
        component = st.text_input("Component (e.g., HP Pump, Cartridge Filter, RO Skid)")
        action = st.text_area("Action / Work Done")
        operator = st.text_input("Technician / Operator")
        notes = st.text_area("Notes", "")

        if st.button("💾 Save Maintenance Record"):
            run_query(
                "INSERT INTO maintenance_log "
                "(maint_date, component, action, operator, notes) "
                "VALUES (%s,%s,%s,%s,%s)",
                (d, component or None, action or None, operator or None, notes or None),
                fetch=False,
            )
            st.success("Maintenance record saved.")

    with col_table:
        st.subheader("Recent Maintenance")
        days_back = st.slider("Show last N days", 30, 365, 90)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT * FROM maintenance_log WHERE maint_date >= %s "
            "ORDER BY maint_date DESC, id DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No maintenance records for selected period.")
        else:
            st.dataframe(df)


def page_system_status():
    apply_theme()
    st.markdown("<div class='top-title'>Pumps & System Status</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>High pressure, low pressure, feed pump & RO running</div>",
                unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Log Status Snapshot")
        now = datetime.datetime.now()
        st.write(f"Log time: **{now}**")
        hp = st.checkbox("High Pressure Pump ON")
        lp = st.checkbox("Low Pressure Pump ON")
        feed = st.checkbox("Feed Pump ON")
        ro = st.checkbox("RO Skid Running")

        if st.button("💾 Save Status Snapshot"):
            run_query(
                "INSERT INTO system_status (hp_pump, lp_pump, feed_pump, ro_running) "
                "VALUES (%s,%s,%s,%s)",
                (hp, lp, feed, ro),
                fetch=False,
            )
            st.success("System status snapshot saved.")

    with col_table:
        st.subheader("Recent Status Log")
        df = fetch_df(
            "SELECT * FROM system_status ORDER BY status_time DESC LIMIT 100"
        )
        if df.empty:
            st.info("No status snapshots logged yet.")
        else:
            st.dataframe(df)
def page_water_quality():
    apply_theme()
    st.markdown("<div class='top-title'>Water Quality Monitoring</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Feed / permeate / reject TDS, pH, conductivity & turbidity</div>",
                unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    sample_points = ["Feed", "Permeate", "Reject"]

    with col_form:
        st.subheader("Log Water Quality Sample")
        d = st.date_input("Sample Date", datetime.date.today())
        t = st.time_input("Sample Time", datetime.datetime.now().time())
        point = st.selectbox("Sampling Point", sample_points)
        tds = st.number_input("TDS (ppm)", min_value=0.0, step=1.0)
        ph = st.number_input("pH", min_value=0.0, max_value=14.0, step=0.1)
        cond = st.number_input("Conductivity (µS/cm)", min_value=0.0, step=1.0)
        turb = st.number_input("Turbidity (NTU)", min_value=0.0, step=0.1)
        operator = st.text_input("Operator / Lab Tech", "")
        notes = st.text_area("Notes", "")

        if st.button("💾 Save Sample"):
            run_query(
                "INSERT INTO water_quality "
                "(sample_date, sample_time, point, tds, ph, conductivity, turbidity, operator, notes) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (d, t, point, tds or None, ph or None, cond or None, turb or None, operator or None, notes or None),
                fetch=False,
            )
            st.success("Water quality sample saved.")

    with col_table:
        st.subheader("Recent Water Quality Samples")
        days_back = st.slider("Show last N days", 7, 90, 30)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT * FROM water_quality WHERE sample_date >= %s "
            "ORDER BY sample_date DESC, sample_time DESC, id DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No water quality data for selected period.")
        else:
            st.dataframe(df)

    st.markdown("---")
    st.subheader("Permeate TDS & pH Trend")

    df_perm = fetch_df(
        "SELECT sample_date, sample_time, tds, ph "
        "FROM water_quality WHERE point=%s ORDER BY sample_date, sample_time",
        ("Permeate",),
    )
    if df_perm.empty:
        st.info("No permeate quality data yet.")
    else:
        df_perm["ts"] = pd.to_datetime(
            df_perm["sample_date"].astype(str) + " " + df_perm["sample_time"].astype(str)
        )
        df_perm = df_perm.set_index("ts")
        col1, col2 = st.columns(2)
        with col1:
            st.caption("Permeate TDS (ppm)")
            st.line_chart(df_perm["tds"])
        with col2:
            st.caption("Permeate pH")
            st.line_chart(df_perm["ph"])


def main():
    st.set_page_config(page_title="Um Qasr RO System", layout="wide", page_icon="💧")
    apply_theme()
    init_db()

    st.sidebar.title("Um Qasr RO System")
    page = st.sidebar.radio(
        "Navigate",
        [
            "Dashboard",
            "Flowmeter Readings",
            "Production Reports",
            "Chemical Movement",
            "Cartridge Filters",
            "Maintenance Logs",
            "System Status",
            "Water Quality",
        ],
    )

    if page == "Dashboard":
        page_dashboard()
    elif page == "Flowmeter Readings":
        page_flowmeter()
    elif page == "Production Reports":
        page_production()
    elif page == "Chemical Movement":
        page_chemicals()
    elif page == "Cartridge Filters":
        page_filters()
    elif page == "Maintenance Logs":
        page_maintenance()
    elif page == "System Status":
        page_system_status()
    elif page == "Water Quality":
        page_water_quality()


if __name__ == "__main__":
    main()
