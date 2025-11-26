# Um Qasr RO System - Nile Projects Service
# Emerald RO Dashboard + Chemicals + Water Quality + CMMS + To-Do

import streamlit as st
import pandas as pd
import datetime
from io import BytesIO

import psycopg2
from psycopg2.extras import RealDictCursor

# =========================
# CONFIG
# =========================

DB_URL = (
    "postgresql://neondb_owner:npg_C4ghxK1yUcfw@"
    "ep-billowing-fog-agxbr2fc-pooler.c-2.eu-central-1.aws.neon.tech/"
    "neondb?sslmode=require&channel_binding=require"
)

# CMMS start of operation (1-8-2025)
CMMS_START_DATE = datetime.date(2025, 8, 1)

# =========================
# DB HELPERS
# =========================

def get_conn():
    return psycopg2.connect(DB_URL)


def run_query(sql, params=None, fetch=False):
    conn = get_conn()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, params or [])
    data = cur.fetchall() if fetch else None
    conn.commit()
    cur.close()
    conn.close()
    return data


def fetch_df(sql, params=None):
    conn = get_conn()
    df = pd.read_sql(sql, conn, params=params)
    conn.close()
    return df


def init_db():
    """Create/upgrade all database tables."""
    ddl_statements = [
        # Flowmeter
        """
        CREATE TABLE IF NOT EXISTS flowmeter_readings (
            id SERIAL PRIMARY KEY,
            reading_date DATE NOT NULL UNIQUE,
            reading_value NUMERIC(12,2) NOT NULL,
            operator VARCHAR(100),
            notes TEXT
        );
        """,
        # Daily production
        """
        CREATE TABLE IF NOT EXISTS daily_production (
            id SERIAL PRIMARY KEY,
            prod_date DATE NOT NULL UNIQUE,
            prod_value NUMERIC(12,2) NOT NULL,
            cumulative_month NUMERIC(12,2),
            cumulative_total NUMERIC(12,2)
        );
        """,
        # Cartridge filters
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
        # Chemical movements
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
        # Chemical stock
        """
        CREATE TABLE IF NOT EXISTS chemicals_stock (
            chemical VARCHAR(50) PRIMARY KEY,
            stock_qty NUMERIC(12,2)
        );
        """,
        # System status
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
        # Simple maintenance log
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
        # Operators / engineers
        """
        CREATE TABLE IF NOT EXISTS operators (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) UNIQUE NOT NULL,
            role VARCHAR(50)
        );
        """,
        # Water quality
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
        """,
        # CMMS master tasks
        """
        CREATE TABLE IF NOT EXISTS maintenance_master (
            id SERIAL PRIMARY KEY,
            task_name TEXT NOT NULL,
            frequency VARCHAR(20) NOT NULL,
            interval_days INTEGER NOT NULL,
            category VARCHAR(20),
            default_priority VARCHAR(20),
            estimated_hours NUMERIC(6,2),
            active BOOLEAN DEFAULT TRUE
        );
        """,
        # CMMS work orders
        """
        CREATE TABLE IF NOT EXISTS maintenance_workorders (
            id SERIAL PRIMARY KEY,
            master_id INTEGER REFERENCES maintenance_master(id),
            due_date DATE NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending',
            priority VARCHAR(20),
            technician VARCHAR(100),
            estimated_hours NUMERIC(6,2),
            actual_hours NUMERIC(6,2),
            cost NUMERIC(12,2),
            completion_date DATE,
            remarks TEXT
        );
        """,
        # Operator To-Do master
        """
        CREATE TABLE IF NOT EXISTS operator_todo_master (
            id SERIAL PRIMARY KEY,
            operator_name VARCHAR(100) NOT NULL,
            title TEXT NOT NULL,
            frequency VARCHAR(20) NOT NULL,
            interval_days INTEGER NOT NULL,
            active BOOLEAN DEFAULT TRUE
        );
        """,
        # Operator To-Do items
        """
        CREATE TABLE IF NOT EXISTS operator_todo_items (
            id SERIAL PRIMARY KEY,
            master_id INTEGER REFERENCES operator_todo_master(id),
            due_date DATE NOT NULL,
            status VARCHAR(20) DEFAULT 'Pending'
        );
        """
    ]

    conn = get_conn()
    cur = conn.cursor()
    for ddl in ddl_statements:
        cur.execute(ddl)

    # Extra columns for chemical cost / value
    cur.execute("ALTER TABLE chemicals_stock ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(12,2);")
    cur.execute("ALTER TABLE chemicals_stock ADD COLUMN IF NOT EXISTS stock_value NUMERIC(12,2);")
    cur.execute("ALTER TABLE chemicals_movement ADD COLUMN IF NOT EXISTS unit_cost NUMERIC(12,2);")
    cur.execute("ALTER TABLE chemicals_movement ADD COLUMN IF NOT EXISTS stock_value NUMERIC(12,2);")

    conn.commit()
    cur.close()
    conn.close()


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
    buffer = BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
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
# EMERALD THEME
# =========================

THEME_CSS = """
<style>
body {
    background: #e7f6f0;
    color: #064e3b;
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
    color: #065f46;
    text-shadow: 0 0 10px rgba(16, 185, 129, 0.35);
    margin-bottom: 0.2rem;
}
.subtitle {
    font-size: 0.95rem;
    color: #047857;
    margin-bottom: 1.5rem;
}
/* KPI cards */
.kpi-card {
    background: linear-gradient(135deg, rgba(16, 185, 129, 0.16), rgba(255, 255, 255, 0.9));
    border-radius: 14px;
    padding: 0.9rem 1.1rem;
    border: 1px solid rgba(16, 185, 129, 0.55);
    box-shadow: 0 8px 20px rgba(4, 120, 87, 0.18);
}
.kpi-label {
    font-size: 0.8rem;
    color: #047857;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
.kpi-value {
    font-size: 1.4rem;
    font-weight: 700;
    color: #022c22;
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
    background: rgba(22, 163, 74, 0.08);
    color: #15803d;
    border: 1px solid rgba(22, 163, 74, 0.55);
}
.status-warn {
    background: rgba(245, 158, 11, 0.12);
    color: #b45309;
    border: 1px solid rgba(245, 158, 11, 0.55);
}
.status-alarm {
    background: rgba(239, 68, 68, 0.12);
    color: #b91c1c;
    border: 1px solid rgba(239, 68, 68, 0.6);
}
/* Emerald chemical tags */
.chem-tag {
    display:inline-block;
    padding:0.15rem 0.6rem;
    border-radius:999px;
    border:1px solid #10b981cc;
    background:rgba(16,185,129,0.18);
    color:#065f46;
    font-size:0.8rem;
    margin-right:0.25rem;
}
/* Sidebar */
section[data-testid="stSidebar"] {
    background: #022c22;
}
section[data-testid="stSidebar"] * {
    color: #e5fff5;
}
/* Tables */
.dataframe tbody tr:nth-child(odd) {
    background-color: #ecfdf5;
}
.dataframe tbody tr:nth-child(even) {
    background-color: #d1fae5;
}
.dataframe thead {
    background-color: #047857;
    color: #ecfdf5;
}
/* Buttons */
.stButton>button, .stDownloadButton>button {
    border-radius: 999px;
    padding: 0.4rem 1.1rem;
    border: 1px solid rgba(16, 185, 129, 0.85);
    background: linear-gradient(135deg, #10b981, #22c55e);
    color: #022c22;
    font-weight: 600;
}
.stButton>button:hover, .stDownloadButton>button:hover {
    filter: brightness(1.05);
    box-shadow: 0 0 12px rgba(16, 185, 129, 0.75);
}
/* Tabs */
button[data-baseweb="tab"] {
    border-radius: 999px !important;
}
</style>
"""


def apply_theme():
    st.markdown(THEME_CSS, unsafe_allow_html=True)
# =========================
# CMMS MASTER DATA (from handbook)
# =========================

FREQUENCY_INFO = {
    "Daily": ("daily", 1),
    "Weekly": ("weekly", 7),
    "Monthly": ("monthly", 30),
    "Quarterly": ("quarterly", 90),
    "Semi-Annual": ("semi_annual", 180),
    "Annual": ("annual", 365),
}

MAINTENANCE_TASKS = [
    # Daily
    ("Daily", "Check TDS, pressure and flow rates via PLC/HMI"),
    ("Daily", "Inspect for leaks in piping and fittings"),
    ("Daily", "Verify chemical dosing levels (HCL, BC, Chlorine)"),
    ("Daily", "Record water quality for feed and permeate (TDS, pH)"),
    # Weekly
    ("Weekly", "Inspect cartridge filters and ΔP; replace if pressure drop > 1 bar"),
    ("Weekly", "Test chlorine level in feed water (target 0.5–1 ppm)"),
    ("Weekly", "Check UV sterilizer lamp and clean quartz sleeve if needed"),
    ("Weekly", "Review incident log for recurring issues"),
    # Monthly
    ("Monthly", "Inspect HP pumps for noise, vibration and overheating"),
    ("Monthly", "Test feed water hardness and adjust antiscalant dosing"),
    ("Monthly", "Backwash calcite / multimedia filters as required"),
    ("Monthly", "Verify PLC/HMI functionality and back up setpoints"),
    # Quarterly
    ("Quarterly", "Replace cartridge filters (or sooner if clogged)"),
    ("Quarterly", "Light chemical cleaning of RO membranes for scaling/fouling"),
    ("Quarterly", "Inspect electrical connections and tighten terminals"),
    # Semi-Annual
    ("Semi-Annual", "Deep chemical cleaning of RO membranes (acid + biocide)"),
    ("Semi-Annual", "Inspect and replace O-rings and seals if worn"),
    ("Semi-Annual", "Calibrate pressure and TDS sensors"),
    # Annual
    ("Annual", "Replace UV sterilizer lamp"),
    ("Annual", "Full system audit including vessels and frame"),
    ("Annual", "Review and update O&M manual if modifications were made"),
]


def seed_maintenance_master():
    """Insert handbook tasks into maintenance_master if empty."""
    row = run_query("SELECT COUNT(*) AS c FROM maintenance_master", fetch=True)[0]
    if row["c"] > 0:
        return

    for category, task_name in MAINTENANCE_TASKS:
        freq_code, interval_days = FREQUENCY_INFO[category]
        run_query(
            """
            INSERT INTO maintenance_master
            (task_name, frequency, interval_days, category, default_priority, estimated_hours, active)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            """,
            (task_name, freq_code, interval_days, category, "Medium", 2.0, True),
            fetch=False,
        )


def generate_cmms_schedule(start_date: datetime.date, days_ahead: int = 365):
    """Generate work orders from master tasks within a window."""
    end_date = start_date + datetime.timedelta(days=days_ahead)
    masters = run_query(
        "SELECT id, interval_days, default_priority FROM maintenance_master WHERE active=TRUE",
        fetch=True,
    )
    if not masters:
        return

    for m in masters:
        mid = m["id"]
        interval = int(m["interval_days"])
        priority = m["default_priority"] or "Medium"
        d = start_date
        while d <= end_date:
            existing = run_query(
                "SELECT id FROM maintenance_workorders WHERE master_id=%s AND due_date=%s LIMIT 1",
                (mid, d),
                fetch=True,
            )
            if not existing:
                run_query(
                    """
                    INSERT INTO maintenance_workorders
                    (master_id, due_date, status, priority, estimated_hours)
                    VALUES (%s,%s,'Pending',%s,%s)
                    """,
                    (mid, d, priority, 2.0),
                    fetch=False,
                )
            d += datetime.timedelta(days=interval)


# =========================
# OPERATOR TO-DO HELPERS
# =========================

def generate_todo_schedule(operator_name: str, days_ahead: int = 60):
    """Generate to-do checklist items for an operator."""
    today = datetime.date.today()
    end_date = today + datetime.timedelta(days=days_ahead)
    masters = run_query(
        """
        SELECT id, interval_days FROM operator_todo_master
        WHERE active=TRUE AND operator_name=%s
        """,
        (operator_name,),
        fetch=True,
    )
    if not masters:
        return

    for m in masters:
        mid = m["id"]
        interval = int(m["interval_days"])
        d = today
        while d <= end_date:
            exists = run_query(
                "SELECT id FROM operator_todo_items WHERE master_id=%s AND due_date=%s LIMIT 1",
                (mid, d),
                fetch=True,
            )
            if not exists:
                run_query(
                    "INSERT INTO operator_todo_items (master_id, due_date, status) VALUES (%s,%s,'Pending')",
                    (mid, d),
                    fetch=False,
                )
            d += datetime.timedelta(days=interval)


# Chemicals list
CHEMICALS = ["HCL", "BC", "Chlorine"]


# =========================
# DASHBOARD PAGE
# =========================

def page_dashboard():
    apply_theme()
    st.markdown("<div class='top-title'>RO Plant – Emerald Dashboard</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Production, cartridges, chemicals, water quality & CMMS overview</div>",
        unsafe_allow_html=True,
    )

    today = datetime.date.today()
    first_month = today.replace(day=1)

    # Production (month & lifetime)
    df_prod = fetch_df(
        "SELECT * FROM daily_production WHERE prod_date >= %s AND prod_date <= %s ORDER BY prod_date",
        (first_month, today),
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

    # Chemical stock alerts
    df_stock = fetch_df(
        "SELECT chemical, COALESCE(stock_qty,0) AS stock_qty FROM chemicals_stock ORDER BY chemical"
    )
    low_chems = []
    for _, row in df_stock.iterrows():
        q = float(row["stock_qty"] or 0)
        lvl = None
        if q <= 0:
            lvl = "Empty"
        elif q < 50:
            lvl = "Critical <50 kg"
        elif q < 100:
            lvl = "Low <100 kg"
        if lvl:
            low_chems.append(f"{row['chemical']}: {lvl} (current {q:.1f} kg)")

    # Latest permeate water quality
    df_wq = fetch_df(
        "SELECT * FROM water_quality WHERE point=%s "
        "ORDER BY sample_date DESC, sample_time DESC, id DESC LIMIT 1",
        ("Permeate",),
    )
    last_tds = last_ph = None
    if not df_wq.empty:
        last_tds = float(df_wq["tds"].iloc[0] or 0)
        last_ph = float(df_wq["ph"].iloc[0] or 0)

    # CMMS counts
    df_overdue = fetch_df(
        "SELECT COUNT(*) AS c FROM maintenance_workorders "
        "WHERE status='Pending' AND due_date < %s",
        (today,),
    )
    df_next14 = fetch_df(
        "SELECT COUNT(*) AS c FROM maintenance_workorders "
        "WHERE status='Pending' AND due_date BETWEEN %s AND %s",
        (today, today + datetime.timedelta(days=14)),
    )
    overdue_count = int(df_overdue["c"].iloc[0]) if not df_overdue.empty else 0
    next14_count = int(df_next14["c"].iloc[0]) if not df_next14.empty else 0

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
                "<div class='kpi-card'><div class='kpi-label'>Cartridge ΔP Status</div>"
                f"<div class='kpi-value'><span class='status-pill {cart_class}'>{cart_status}</span></div></div>",
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                "<div class='kpi-card'><div class='kpi-label'>Cartridge ΔP Status</div>"
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
        st.markdown(
            "<div class='kpi-card'><div class='kpi-label'>CMMS</div>"
            f"<div class='kpi-value'>{overdue_count} OD / {next14_count} Next 14d</div></div>",
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
# =========================
# FLOWMETER & PRODUCTION PAGES
# =========================

def page_flowmeter():
    apply_theme()
    st.markdown("<div class='top-title'>Flowmeter Readings</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Daily RO product totalizer readings</div>", unsafe_allow_html=True)

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
                DO UPDATE SET reading_value=EXCLUDED.reading_value,
                              operator=EXCLUDED.operator,
                              notes=EXCLUDED.notes;
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
            st.info("No readings for selected period.")
        else:
            st.dataframe(df)

    st.markdown("---")
    st.subheader("Generate Daily Production from Flowmeter")

    if st.button("⚙️ Recalculate Daily Production from all readings"):
        df_all = fetch_df("SELECT * FROM flowmeter_readings ORDER BY reading_date")
        if df_all.shape[0] < 2:
            st.warning("Need at least 2 readings to calculate daily production.")
        else:
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

                if current_month is None or d.month != current_month.month or d.year != current_month.year:
                    cumulative_month = prod
                    current_month = d
                else:
                    cumulative_month += prod
                cumulative_total += prod

                run_query(
                    """
                    INSERT INTO daily_production
                    (prod_date, prod_value, cumulative_month, cumulative_total)
                    VALUES (%s,%s,%s,%s)
                    """,
                    (d, prod, cumulative_month, cumulative_total),
                    fetch=False,
                )
            st.success("Daily production recalculated from flowmeter readings.")


def page_production():
    apply_theme()
    st.markdown("<div class='top-title'>Production Reports</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Daily, monthly and cumulative production</div>", unsafe_allow_html=True)

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

    buf_x = export_df_to_excel(df, sheet_name="Production")
    st.download_button(
        "⬇️ Download production_report.xlsx",
        data=buf_x.getvalue(),
        file_name="production_report.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

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


# =========================
# CHEMICALS – STOCK, COST & MOVEMENTS (HCL / BC / Chlorine)
# =========================

def page_chemicals():
    apply_theme()
    st.markdown("<div class='top-title'>Chemicals – Stock, Cost & Movements</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Track HCL, BC, Chlorine stock, unit cost and IN / OUT movements.</div>",
        unsafe_allow_html=True,
    )

    st.markdown(
        "<p><span class='chem-tag'>HCL</span>"
        "<span class='chem-tag'>BC (Biocide)</span>"
        "<span class='chem-tag'>Chlorine</span></p>",
        unsafe_allow_html=True,
    )

    tab_stock, tab_inout, tab_hist = st.tabs(
        ["📦 Stock & Cost", "➕ Record IN / OUT", "📜 Movements History"]
    )

    # TAB 1 – Stock & Cost
    with tab_stock:
        st.subheader("Current Stock and Value")
        df_stock = fetch_df(
            "SELECT chemical AS name, stock_qty AS qty, "
            "COALESCE(unit_cost,0) AS unit_cost, COALESCE(stock_value,0) AS stock_value "
            "FROM chemicals_stock ORDER BY chemical"
        )
        if df_stock.empty:
            # Ensure 3 chemicals exist
            for ch in CHEMICALS:
                run_query(
                    """
                    INSERT INTO chemicals_stock (chemical, stock_qty, unit_cost, stock_value)
                    VALUES (%s,0,0,0)
                    ON CONFLICT (chemical) DO NOTHING;
                    """,
                    (ch,),
                    fetch=False,
                )
            df_stock = fetch_df(
                "SELECT chemical AS name, stock_qty AS qty, "
                "COALESCE(unit_cost,0) AS unit_cost, COALESCE(stock_value,0) AS stock_value "
                "FROM chemicals_stock ORDER BY chemical"
            )
        st.dataframe(df_stock)

        st.markdown("### Update Unit Cost (per kg)")
        col_c1, col_c2, col_c3 = st.columns([2, 1, 1])
        with col_c1:
            chem_sel = st.selectbox("Chemical", CHEMICALS)
        with col_c2:
            new_cost = st.number_input("Unit Cost (per kg)", min_value=0.0, step=0.1)
        with col_c3:
            if st.button("💾 Save Cost"):
                row = fetch_df(
                    "SELECT stock_qty FROM chemicals_stock WHERE chemical=%s",
                    (chem_sel,),
                )
                qty = float(row["stock_qty"].iloc[0]) if not row.empty else 0.0
                stock_val = qty * new_cost
                run_query(
                    """
                    INSERT INTO chemicals_stock (chemical, stock_qty, unit_cost, stock_value)
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (chemical)
                    DO UPDATE SET stock_qty=EXCLUDED.stock_qty,
                                  unit_cost=EXCLUDED.unit_cost,
                                  stock_value=EXCLUDED.stock_value;
                    """,
                    (chem_sel, qty, new_cost, stock_val),
                    fetch=False,
                )
                st.success(f"Cost for {chem_sel} updated to {new_cost:.2f}.")

    # TAB 2 – Record IN/OUT
    with tab_inout:
        st.subheader("Record IN / OUT Movement")
        col_f1, col_f2 = st.columns(2)
        with col_f1:
            m_date = st.date_input("Date", datetime.date.today())
            chem = st.selectbox("Chemical", CHEMICALS, key="chem_move")
            qty_in = st.number_input("Qty IN (kg)", min_value=0.0, step=0.1)
            qty_out = st.number_input("Qty OUT (kg)", min_value=0.0, step=0.1)
        with col_f2:
            unit_cost = st.number_input("Unit Cost for this movement (per kg)", min_value=0.0, step=0.1)
            operator = st.text_input("Operator", "")
            notes = st.text_area("Notes", "")

        if st.button("💾 Save Movement"):
            df_last = fetch_df(
                "SELECT balance, unit_cost FROM chemicals_movement WHERE chemical=%s "
                "ORDER BY movement_date DESC, id DESC LIMIT 1",
                (chem,),
            )
            last_bal = float(df_last["balance"].iloc[0]) if not df_last.empty else 0.0
            last_cost = float(df_last["unit_cost"].iloc[0]) if not df_last.empty else 0.0

            eff_cost = unit_cost if unit_cost > 0 else last_cost
            new_bal = last_bal + qty_in - qty_out
            stock_val = new_bal * eff_cost

            run_query(
                """
                INSERT INTO chemicals_movement
                (movement_date, chemical, qty_in, qty_out, balance,
                 unit_cost, stock_value, operator, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (m_date, chem, qty_in or None, qty_out or None,
                 new_bal, eff_cost, stock_val, operator or None, notes or None),
                fetch=False,
            )

            run_query(
                """
                INSERT INTO chemicals_stock (chemical, stock_qty, unit_cost, stock_value)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (chemical)
                DO UPDATE SET stock_qty=EXCLUDED.stock_qty,
                              unit_cost=EXCLUDED.unit_cost,
                              stock_value=EXCLUDED.stock_value;
                """,
                (chem, new_bal, eff_cost, stock_val),
                fetch=False,
            )
            st.success(
                f"Movement saved. New balance for {chem}: {new_bal:.2f} kg "
                f"(value {stock_val:.2f})."
            )

    # TAB 3 – History
    with tab_hist:
        st.subheader("Movements History")
        days_back = st.slider("Show last N days", 7, 180, 60)
        start_date = datetime.date.today() - datetime.timedelta(days=days_back)
        df = fetch_df(
            "SELECT movement_date, chemical, qty_in, qty_out, balance, "
            "unit_cost, stock_value, operator, notes "
            "FROM chemicals_movement WHERE movement_date >= %s "
            "ORDER BY movement_date DESC, id DESC",
            (start_date,),
        )
        if df.empty:
            st.info("No chemical movements for selected period.")
        else:
            st.dataframe(df)


# =========================
# CARTRIDGE FILTERS PAGE
# =========================

def page_filters():
    apply_theme()
    st.markdown("<div class='top-title'>Cartridge Filter ΔP</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Pressure before & after, differential and condition status.</div>",
        unsafe_allow_html=True,
    )

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
            msg = "Monitor filter – getting loaded."
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
                """
                INSERT INTO cartridge_filters
                (entry_date, pressure_before, pressure_after, diff_pressure,
                 status, operator, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                """,
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
# =========================
# SIMPLE MAINTENANCE LOG
# =========================

def page_maintenance_log():
    apply_theme()
    st.markdown("<div class='top-title'>Maintenance Log</div>", unsafe_allow_html=True)
    st.markdown("<div class='subtitle'>Record corrective or extra maintenance not in CMMS.</div>",
                unsafe_allow_html=True)

    col_form, col_table = st.columns([1, 2])

    with col_form:
        st.subheader("Log Maintenance")
        d = st.date_input("Maintenance Date", datetime.date.today())
        component = st.text_input("Component (e.g. HP Pump, RO Skid)")
        action = st.text_area("Action / Work Done")
        operator = st.text_input("Technician / Operator")
        notes = st.text_area("Notes", "")

        if st.button("💾 Save Maintenance Record"):
            run_query(
                """
                INSERT INTO maintenance_log
                (maint_date, component, action, operator, notes)
                VALUES (%s,%s,%s,%s,%s)
                """,
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


# =========================
# SYSTEM STATUS PAGE
# =========================

def page_system_status():
    apply_theme()
    st.markdown("<div class='top-title'>Pumps & System Status</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>HP, LP, feed pumps and RO skid running status.</div>",
        unsafe_allow_html=True,
    )

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
                """
                INSERT INTO system_status (hp_pump, lp_pump, feed_pump, ro_running)
                VALUES (%s,%s,%s,%s)
                """,
                (hp, lp, feed, ro),
                fetch=False,
            )
            st.success("System status snapshot saved.")

    with col_table:
        st.subheader("Recent Status Log")
        df = fetch_df("SELECT * FROM system_status ORDER BY status_time DESC LIMIT 100")
        if df.empty:
            st.info("No status snapshots logged yet.")
        else:
            st.dataframe(df)


# =========================
# WATER QUALITY PAGE
# =========================

def page_water_quality():
    apply_theme()
    st.markdown("<div class='top-title'>Water Quality Monitoring</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Feed / permeate / reject TDS, pH, conductivity & turbidity.</div>",
        unsafe_allow_html=True,
    )

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
                """
                INSERT INTO water_quality
                (sample_date, sample_time, point, tds, ph, conductivity, turbidity,
                 operator, notes)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (d, t, point, tds or None, ph or None, cond or None, turb or None,
                 operator or None, notes or None),
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
# =========================
# ADVANCED CMMS PAGE (WORK ORDERS)
# =========================

def page_cmms():
    apply_theme()
    st.markdown("<div class='top-title'>RO CMMS – Work Orders</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Handbook-based maintenance schedule with priorities, costs and completion tracking.</div>",
        unsafe_allow_html=True,
    )

    col_left, col_right = st.columns([1, 2])

    # ---------- Scheduler control ----------
    with col_left:
        st.subheader("Scheduler Control")

        if st.button("Seed Master Tasks (from Handbook)"):
            seed_maintenance_master()
            st.success("Master tasks seeded / already present.")

        st.write(f"CMMS start date: **{CMMS_START_DATE}**")
        days_ahead = st.number_input("Generate schedule days ahead", 30, 730, 365, step=30)

        if st.button("Generate / Refresh Schedule"):
            seed_maintenance_master()
            generate_cmms_schedule(CMMS_START_DATE, days_ahead=int(days_ahead))
            st.success(f"Schedule generated from {CMMS_START_DATE} for {days_ahead} days.")

    # ---------- Overview ----------
    with col_right:
        st.subheader("Overview")

        today = datetime.date.today()

        overdue = fetch_df(
            """
            SELECT w.id, m.task_name, w.due_date, w.priority
            FROM maintenance_workorders w
            JOIN maintenance_master m ON w.master_id = m.id
            WHERE w.status = 'Pending' AND w.due_date < %s
            ORDER BY w.due_date
            """,
            (today,),
        )

        upcoming = fetch_df(
            """
            SELECT w.id, m.task_name, w.due_date, w.priority
            FROM maintenance_workorders w
            JOIN maintenance_master m ON w.master_id = m.id
            WHERE w.status = 'Pending' AND w.due_date BETWEEN %s AND %s
            ORDER BY w.due_date
            """,
            (today, today + datetime.timedelta(days=14)),
        )

        completed = fetch_df(
            """
            SELECT w.id, m.task_name, w.due_date, w.completion_date, w.cost
            FROM maintenance_workorders w
            JOIN maintenance_master m ON w.master_id = m.id
            WHERE w.status = 'Completed' AND w.completion_date >= %s
            ORDER BY w.completion_date DESC
            """,
            (today - datetime.timedelta(days=30),),
        )

        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Overdue", len(overdue))
        with c2:
            st.metric("Due in next 14 days", len(upcoming))
        with c3:
            st.metric("Completed last 30 days", len(completed))

        tabs = st.tabs(["Overdue", "Upcoming", "Completed"])
        with tabs[0]:
            st.caption("Overdue Work Orders")
            st.dataframe(overdue)
        with tabs[1]:
            st.caption("Upcoming Work Orders (14 days)")
            st.dataframe(upcoming)
        with tabs[2]:
            st.caption("Recently Completed (30 days)")
            st.dataframe(completed)

    st.markdown("---")
    st.subheader("Update Work Order")

    open_df = fetch_df(
        """
        SELECT w.id, m.task_name, w.due_date, w.priority, w.status
        FROM maintenance_workorders w
        JOIN maintenance_master m ON w.master_id = m.id
        ORDER BY w.due_date
        """
    )

    if open_df.empty:
        st.info("No work orders yet. Generate schedule first.")
        return

    open_df["label"] = open_df.apply(
        lambda r: f"{r['id']} | {r['due_date']} | {r['task_name'][:40]}...", axis=1
    )

    selected_label = st.selectbox("Select Work Order", open_df["label"].tolist())
    sel_id = int(selected_label.split("|")[0].strip())
    sel_row = open_df[open_df["id"] == sel_id].iloc[0]

    st.write(f"**Task:** {sel_row['task_name']}")
    st.write(f"**Due date:** {sel_row['due_date']}")

    status_new = st.selectbox("Status", ["Pending", "Completed", "Cancelled"], index=0)
    priority_new = st.selectbox("Priority", ["Low", "Medium", "High", "Critical"], index=1)
    tech = st.text_input("Technician")
    est_hours = st.number_input("Estimated Hours", min_value=0.0, step=0.5, value=2.0)
    act_hours = st.number_input("Actual Hours", min_value=0.0, step=0.5, value=0.0)
    cost = st.number_input("Cost (USD)", min_value=0.0, step=1.0, value=0.0)
    remarks = st.text_area("Remarks / Actions Taken")

    if st.button("💾 Save Work Order Update"):
        completion_date = datetime.date.today() if status_new == "Completed" else None
        run_query(
            """
            UPDATE maintenance_workorders
            SET status=%s,
                priority=%s,
                technician=%s,
                estimated_hours=%s,
                actual_hours=%s,
                cost=%s,
                completion_date=%s,
                remarks=%s
            WHERE id=%s
            """,
            (
                status_new,
                priority_new,
                tech or None,
                est_hours,
                act_hours or None,
                cost or None,
                completion_date,
                remarks or None,
                sel_id,
            ),
            fetch=False,
        )
        st.success("Work order updated.")


# =========================
# OPERATOR TO-DO LIST PAGE
# =========================

def page_todo():
    apply_theme()
    st.markdown("<div class='top-title'>Operator To-Do List</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Engineers define daily / weekly / monthly tasks, operators see checklists.</div>",
        unsafe_allow_html=True,
    )

    st.subheader("Operators & Recurring Tasks")

    col_a, col_b = st.columns(2)

    # ----- Add operator -----
    with col_a:
        op_name = st.text_input("Add new operator / engineer")
        if st.button("➕ Add Operator"):
            if op_name.strip():
                run_query(
                    """
                    INSERT INTO operators (name, role)
                    VALUES (%s,%s)
                    ON CONFLICT (name) DO NOTHING
                    """,
                    (op_name.strip(), "Engineer"),
                    fetch=False,
                )
                st.success(f"Operator {op_name} added.")

    with col_b:
        df_ops = fetch_df("SELECT name, role FROM operators ORDER BY name")
        if df_ops.empty:
            st.info("No operators yet. Add one on the left.")
        else:
            st.dataframe(df_ops)

    st.markdown("---")

    df_ops = fetch_df("SELECT name FROM operators ORDER BY name")
    if df_ops.empty:
        st.warning("Add at least one operator to start defining to-do tasks.")
        return

    operator_selected = st.selectbox("Select operator for to-do list", df_ops["name"].tolist())

    tab_master, tab_today, tab_upcoming = st.tabs(
        ["Define Recurring Tasks", "Today's Checklist", "Upcoming"]
    )

    # ----- Tab 1: define recurring tasks -----
    with tab_master:
        st.subheader("New recurring task")

        title = st.text_input("Task title (e.g. 'Check RO skid drains')")
        freq_label = st.selectbox("Frequency", ["Daily", "Weekly", "Monthly"])
        interval = {"Daily": 1, "Weekly": 7, "Monthly": 30}[freq_label]

        if st.button("💾 Save recurring task"):
            if title.strip():
                run_query(
                    """
                    INSERT INTO operator_todo_master
                    (operator_name, title, frequency, interval_days, active)
                    VALUES (%s,%s,%s,%s,TRUE)
                    """,
                    (operator_selected, title.strip(), freq_label, interval),
                    fetch=False,
                )
                st.success("Recurring task saved.")

        df_master = fetch_df(
            """
            SELECT id, title, frequency, interval_days, active
            FROM operator_todo_master
            WHERE operator_name=%s
            ORDER BY id
            """,
            (operator_selected,),
        )
        if df_master.empty:
            st.info("No tasks yet for this operator.")
        else:
            st.dataframe(df_master)

        if st.button("⚙️ Generate schedule for this operator"):
            generate_todo_schedule(operator_selected, days_ahead=60)
            st.success("To-do schedule generated for next 60 days.")

    # ----- Tab 2: today's checklist -----
    with tab_today:
        st.subheader("Checklist")
        date_sel = st.date_input("Checklist date", datetime.date.today())
        df_items = fetch_df(
            """
            SELECT i.id, m.title, i.status
            FROM operator_todo_items i
            JOIN operator_todo_master m ON i.master_id = m.id
            WHERE m.operator_name=%s AND i.due_date=%s
            ORDER BY i.id
            """,
            (operator_selected, date_sel),
        )

        if df_items.empty:
            st.info("No to-do items for this date. Generate schedule if needed.")
        else:
            for _, row in df_items.iterrows():
                checked = row["status"] == "Completed"
                new_val = st.checkbox(row["title"], value=checked, key=f"todo_{row['id']}")
                new_status = "Completed" if new_val else "Pending"
                if new_status != row["status"]:
                    run_query(
                        "UPDATE operator_todo_items SET status=%s WHERE id=%s",
                        (new_status, int(row["id"])),
                        fetch=False,
                    )

    # ----- Tab 3: upcoming -----
    with tab_upcoming:
        st.subheader("Upcoming Tasks (next 14 days)")
        today = datetime.date.today()
        df_upc = fetch_df(
            """
            SELECT i.due_date, m.title, i.status
            FROM operator_todo_items i
            JOIN operator_todo_master m ON i.master_id = m.id
            WHERE m.operator_name=%s
              AND i.due_date BETWEEN %s AND %s
            ORDER BY i.due_date
            """,
            (operator_selected, today, today + datetime.timedelta(days=14)),
        )
        if df_upc.empty:
            st.info("No upcoming tasks. Generate schedule to populate.")
        else:
            st.dataframe(df_upc)


# =========================
# OPERATION MANUAL PAGE
# =========================

def page_operation_manual():
    apply_theme()
    st.markdown("<div class='top-title'>Operation & Maintenance Manual</div>", unsafe_allow_html=True)
    st.markdown(
        "<div class='subtitle'>Embedded quick-reference manual for operators based on the handover handbook.</div>",
        unsafe_allow_html=True,
    )

    st.info(
        "This is a concise O&M summary. The official detailed manual must remain in the project handover files."
    )

    with st.expander("1. System Overview"):
        st.markdown(
            """
            - Feed water → pre-treatment → RO skid → product tank.  
            - Key monitored parameters:
              - Feed / permeate **TDS** and **pH**  
              - Pressure (feed, HP, concentrate, cartridge ΔP)  
              - Flow rates (feed, permeate, reject)  
              - Chemical dosing: **HCL**, **BC (biocide)**, **Chlorine**.  
            - PLC/HMI provides alarms and interlocks for safe operation.
            """
        )

    with st.expander("2. Start-up Procedure"):
        st.markdown(
            """
            1. Confirm valves in normal position and sufficient product tank capacity.  
            2. Ensure HCL, BC and Chlorine tanks have enough stock and dosing pumps are primed.  
            3. Start feed pump and verify stable suction / discharge pressure.  
            4. Start high pressure pump and ramp pressure gradually to design setpoint.  
            5. Check permeate flow, reject flow and recovery; confirm no abnormal vibration or noise.  
            6. Verify permeate TDS within target limit before sending to service tank.
            """
        )

    with st.expander("3. Normal Operation Checks"):
        st.markdown(
            """
            - Hourly check (logged in app):
              - RO inlet / outlet pressures, cartridge filter ΔP.  
              - Feed & permeate TDS, pH, conductivity.  
              - Dosing flow rates of HCL, BC, Chlorine and chemical stock levels.  
            - Follow **Daily / Weekly / Monthly** tasks generated in **CMMS** and **To-Do List**.  
            - Record all readings in the **Production** and **Water Quality** pages.
            """
        )

    with st.expander("4. Shutdown Procedure"):
        st.markdown(
            """
            **Short stop (<24 h):**  
            - Stop HP pump, then feed pump.  
            - Keep system hydraulically stable; avoid frequent on/off cycling.  

            **Long stop (>24 h):**  
            - Apply membrane preservation as recommended by supplier (chemical layup).  
            - Flush with low TDS water if available.  
            - Record preservation start in CMMS and schedule recommissioning task.
            """
        )

    with st.expander("5. Emergency & Alarms"):
        st.markdown(
            """
            - **High pressure / low flow:**  
              - Check cartridge filter ΔP, strainers, pump suction, and blockages.  
            - **High product TDS:**  
              - Check mixing, bypasses, membrane fouling or damage; plan cleaning or replacement.  
            - **Chemical dosing failure:**  
              - Switch to standby pump if available; check tanks, lines, injectors.  

            All incidents must be logged in the **Maintenance Log** and linked to a CMMS work order.
            """
        )


# =========================
# MAIN ENTRY POINT
# =========================

def main():
    st.set_page_config(page_title="Um Qasr RO System", layout="wide", page_icon="💧")
    apply_theme()
    init_db()
    seed_maintenance_master()

    st.sidebar.title("Um Qasr RO System – Emerald Unit")
    page = st.sidebar.radio(
        "Navigate",
        [
            "Dashboard",
            "Flowmeter Readings",
            "Production Reports",
            "Chemical Movement",
            "Cartridge Filters",
            "Water Quality",
            "System Status",
            "Maintenance Log",
            "Maintenance CMMS",
            "To-Do List",
            "Operation Manual",
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
    elif page == "Water Quality":
        page_water_quality()
    elif page == "System Status":
        page_system_status()
    elif page == "Maintenance Log":
        page_maintenance_log()
    elif page == "Maintenance CMMS":
        page_cmms()
    elif page == "To-Do List":
        page_todo()
    elif page == "Operation Manual":
        page_operation_manual()


if __name__ == "__main__":
    main()
