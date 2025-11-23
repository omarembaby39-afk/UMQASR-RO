# CHUNK 1 - INITIAL SETUP, IMPORTS, HELPERS

import streamlit as st
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from datetime import date, datetime
from fpdf import FPDF
import os
import shutil
import numpy as np

DB = "ro_uaq.db"

# ----------------- BASIC DB CONNECTION -----------------
def conn():
    return sqlite3.connect(DB, check_same_thread=False)

# ----------------- BACKUP SYSTEM -----------------------
def backup_db_if_needed():
    if not os.path.exists(DB):
        return
    today_str = datetime.now().strftime("%Y%m%d")
    backup_dir = "backup"
    os.makedirs(backup_dir, exist_ok=True)
    backup_path = os.path.join(backup_dir, f"ro_uaq_{today_str}.db")
    if not os.path.exists(backup_path):
        shutil.copyfile(DB, backup_path)

# ----------------- SAFE COLUMN ADD ----------------------
def add_column_if_not_exists(cur, table, column, col_def):
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    if column in cols:
        return
    sql = f"ALTER TABLE {table} ADD COLUMN {column} {col_def}"
    try:
        cur.execute(sql)
    except sqlite3.OperationalError as e:
        print(f"[WARN] Could not add column {column} to {table}: {e}")
# CHUNK 2 - DATABASE INITIALIZATION

def init_db():
    backup_db_if_needed()

    c = conn()
    cur = c.cursor()

    # ---- META TABLE ----
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS meta(
            key TEXT PRIMARY KEY,
            value TEXT
        )
        '''
    )

    # ---- READINGS TABLE ----
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS readings(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT,
            tds REAL,
            ph REAL,
            conductivity REAL,
            flow_m3 REAL,
            production REAL,
            maintenance TEXT,
            notes TEXT
        )
        '''
    )

    # ---- CLEAN REBUILD CARTRIDGE TABLE ----
    cur.execute("DROP TABLE IF EXISTS cartridge")
    cur.execute(
        '''
        CREATE TABLE cartridge(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT,
            dp REAL,
            remarks TEXT,
            is_change INTEGER DEFAULT 0,
            change_cost REAL DEFAULT 0
        )
        '''
    )

    # ---- CHEMICAL STOCK ----
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS chemicals(
            name TEXT PRIMARY KEY,
            qty REAL
        )
        '''
    )
    add_column_if_not_exists(cur, "chemicals", "unit_cost", "REAL DEFAULT 0")

    # ---- CHEMICAL MOVEMENTS ----
    cur.execute(
        '''
        CREATE TABLE IF NOT EXISTS chemical_movements(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            d TEXT,
            name TEXT,
            movement_type TEXT,
            qty REAL,
            remarks TEXT
        )
        '''
    )

    # Ensure base chemicals
    for chem in ["Chlorine", "HCL", "BC"]:
        cur.execute(
            "INSERT OR IGNORE INTO chemicals(name, qty) VALUES(?, ?)",
            (chem, 0.0),
        )

    # Safety: fill missing unit_costs
    cur.execute("UPDATE chemicals SET unit_cost = 0 WHERE unit_cost IS NULL")

    # ---- MONTHLY CHEMICAL RESET ----
    this_month = datetime.now().strftime("%Y-%m")
    cur.execute("SELECT value FROM meta WHERE key = 'chem_last_reset_month'")
    row = cur.fetchone()
    last_month = row[0] if row else None

    if last_month != this_month:
        cur.execute("UPDATE chemicals SET qty = 0.0")
        if last_month is None:
            cur.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?)",
                ("chem_last_reset_month", this_month),
            )
        else:
            cur.execute(
                "UPDATE meta SET value = ? WHERE key = ?",
                (this_month, "chem_last_reset_month"),
            )

    c.commit()
    c.close()
# -----------------------------------------------------------
# CHUNK 3 - DATA FUNCTIONS & COMPLIANCE
# -----------------------------------------------------------

def add_reading(d, tds, ph, cond, flow_m3, prod, maint, notes):
    """Insert one daily RO reading row."""
    c = conn()
    cur = c.cursor()
    cur.execute(
        """
        INSERT INTO readings(d, tds, ph, conductivity, flow_m3, production, maintenance, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (d, tds, ph, cond, flow_m3, prod, maint, notes),
    )
    c.commit()
    c.close()


def get_readings():
    """Return all daily readings as a DataFrame (sorted by date)."""
    c = conn()
    df = pd.read_sql("SELECT * FROM readings ORDER BY d", c)
    c.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"])
    return df


def add_cartridge(d, dp, remarks, is_change=0, change_cost=0.0):
    """Insert one cartridge DP log entry."""
    c = conn()
    cur = c.cursor()
    cur.execute(
        """
        INSERT INTO cartridge(d, dp, remarks, is_change, change_cost)
        VALUES (?, ?, ?, ?, ?)
        """,
        (d, dp, remarks, int(is_change), change_cost),
    )
    c.commit()
    c.close()


def get_cartridge():
    """Return all cartridge records as DataFrame."""
    c = conn()
    df = pd.read_sql("SELECT * FROM cartridge ORDER BY d", c)
    c.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"])
        if "is_change" not in df.columns:
            df["is_change"] = 0
        if "change_cost" not in df.columns:
            df["change_cost"] = 0.0
    return df


def get_chem():
    """Return chemical stock (with unit_cost) as DataFrame."""
    c = conn()
    df = pd.read_sql("SELECT * FROM chemicals", c)
    c.close()
    if "unit_cost" not in df.columns:
        df["unit_cost"] = 0.0
    return df


def update_chemical_cost(name, unit_cost):
    """Update unit cost (per kg) for a chemical."""
    c = conn()
    cur = c.cursor()
    cur.execute(
        "UPDATE chemicals SET unit_cost = ? WHERE name = ?",
        (unit_cost, name),
    )
    c.commit()
    c.close()


def record_chemical_movement(d, name, movement_type, qty, remarks):
    """
    Record chemical IN / OUT movement and update stock.
    movement_type: "IN" = delivery, "OUT" = consumption.
    """
    c = conn()
    cur = c.cursor()

    # Log movement
    cur.execute(
        """
        INSERT INTO chemical_movements(d, name, movement_type, qty, remarks)
        VALUES (?, ?, ?, ?, ?)
        """,
        (d, name, movement_type, qty, remarks),
    )

    # Update stock
    if movement_type == "IN":
        cur.execute(
            "UPDATE chemicals SET qty = qty + ? WHERE name = ?",
            (qty, name),
        )
    elif movement_type == "OUT":
        cur.execute("SELECT qty FROM chemicals WHERE name = ?", (name,))
        row = cur.fetchone()
        current_qty = row[0] if row else 0.0
        new_qty = current_qty - qty
        if new_qty < 0:
            new_qty = 0.0
        cur.execute(
            "UPDATE chemicals SET qty = ? WHERE name = ?",
            (new_qty, name),
        )

    c.commit()
    c.close()


def get_chemical_movements():
    """Return all chemical movements as DataFrame."""
    c = conn()
    df = pd.read_sql(
        "SELECT * FROM chemical_movements ORDER BY d", c
    )
    c.close()
    if len(df) > 0:
        df["d"] = pd.to_datetime(df["d"])
    return df


def compute_compliance(df):
    """
    Compute water quality compliance based on TDS and pH.
    TDS <= 50 ppm and 6.5 <= pH <= 8.5 = in-spec.
    Returns: (compliance%, good_days, bad_days)
    """
    if len(df) == 0:
        return 0.0, 0, 0

    cond_ok = (df["tds"] <= 50) & (df["ph"].between(6.5, 8.5))
    total_days = len(df)
    good_days = int(cond_ok.sum())
    bad_days = int(total_days - good_days)
    compliance = (good_days / total_days) * 100 if total_days else 0
    return round(compliance, 1), good_days, bad_days
# -----------------------------------------------------------
# CHUNK 4 - PDF REPORTS & DIFFERENTIAL PRESSURE GAUGE
# -----------------------------------------------------------

# ---------------------- PDF: Monthly Water + Chemical Report ----------------------
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
    out_days = ((df_month["tds"] > 50) | (~df_month["ph"].between(6.5, 8.5))).sum()
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

    chem_df = get_chem()
    cost_map = {row["name"]: float(row.get("unit_cost", 0.0)) for _, row in chem_df.iterrows()}

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


# ---------------------- PDF: Maintenance Report ----------------------
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


# ---------------------- DIFFERENTIAL PRESSURE (DP) GAUGE ----------------------
def render_dp_gauge(dp_value):
    """
    Renders a compact gauge (0–10 bar) with color zones:
      - Green: DP < 1 bar
      - Orange: 1–4 bar
      - Red: > 4 bar
    """
    dp = max(0.0, min(dp_value, 10.0))

    # Needle color
    if dp < 1:
        needle_color = "green"
    elif dp <= 4:
        needle_color = "orange"
    else:
        needle_color = "red"

    fig, ax = plt.subplots(figsize=(2.2, 2.2))
    ax.set_xlim(-1, 1)
    ax.set_ylim(-1, 1)
    ax.axis("off")

    # Semi-circle
    ang = np.linspace(-0.75 * np.pi, 0.75 * np.pi, 300)
    ax.plot(np.cos(ang), np.sin(ang), linewidth=2)

    # Needle
    angle = -0.75 * np.pi + (dp / 10) * (1.5 * np.pi)
    nx = 0.8 * np.cos(angle)
    ny = 0.8 * np.sin(angle)
    ax.plot([0, nx], [0, ny], linewidth=3, color=needle_color)

    # Center
    ax.plot(0, 0, "o", markersize=8)

    ax.text(0, -0.2, f"{dp:.2f} bar", ha="center", fontsize=10)
    ax.text(0, -0.38, "DP", ha="center", fontsize=8)

    st.pyplot(fig)
# -----------------------------------------------------------
# CHUNK 5 - DASHBOARD AND MAIN DATA ENTRY PAGES
# -----------------------------------------------------------

def page_dashboard():
    st.title("RO System Dashboard - Um Qasr Port")
    st.caption("Unit Capacity: 10 m3/hr - Single RO Unit")

    df = get_readings()
    chem = get_chem()
    cart = get_cartridge()

    # ---------- TOP KPIs ----------
    if len(df) == 0:
        col0, _ = st.columns(2)
        col0.metric("Unit Capacity", "10 m3/hr")
        st.info("No readings data yet.")
    else:
        last_row = df.iloc[-1]
        df30 = df[df["d"] >= datetime.now() - pd.Timedelta(days=30)]
        compliance_30, good_days, bad_days = compute_compliance(df30)
        total_prod_30 = df30["production"].sum()

        col0, col1, col2, col3 = st.columns(4)
        col0.metric("Unit Capacity", "10 m3/hr")
        col1.metric("Last TDS (ppm)", f"{last_row['tds']:.1f}")
        col2.metric("Last pH", f"{last_row['ph']:.2f}")
        col3.metric("Total Records", len(df))

        col4, col5, col6, col7 = st.columns(4)
        col4.metric("30d Compliance (%)", f"{compliance_30:.1f}")
        col5.metric("30d Days In-Spec", good_days)
        col6.metric("30d Days Out-of-Spec", bad_days)
        col7.metric("30d Production (m3)", f"{total_prod_30:.1f}")

        # ---------- TRENDS ----------
        st.markdown("### Water Quality and Production (Last 30 Days)")
        col_left, col_right = st.columns(2)

        with col_left:
            if len(df30) > 0:
                fig, ax = plt.subplots(figsize=(4, 2.5))
                ax.plot(df30["d"], df30["tds"], label="TDS")
                ax.plot(df30["d"], df30["ph"], label="pH")
                ax.plot(df30["d"], df30["conductivity"], label="Cond")
                ax.legend()
                plt.xticks(rotation=45)
                st.pyplot(fig)
            else:
                st.info("No data for last 30 days.")

        with col_right:
            if len(df30) > 0:
                fig2, ax2 = plt.subplots(figsize=(4, 2.5))
                ax2.bar(df30["d"], df30["production"])
                ax2.set_ylabel("Production (m3)")
                plt.xticks(rotation=45)
                st.pyplot(fig2)
            else:
                st.info("No production data for last 30 days.")

    # ---------- DP GAUGE ----------
    st.markdown("---")
    st.subheader("Cartridge Differential Pressure (DP) Gauge 0-10 bar")
    if len(cart) == 0:
        st.info("No cartridge DP data yet.")
    else:
        latest_dp = float(cart["dp"].iloc[-1])
        st.write(f"Latest recorded DP: **{latest_dp:.2f} bar**")
        render_dp_gauge(latest_dp)

        if latest_dp < 1:
            st.success("DP Normal, cartridge OK (DP < 1 bar).")
        elif latest_dp <= 4:
            st.warning("Warning zone, monitor cartridge (1-4 bar).")
        else:
            st.error("High DP > 4 bar, cartridge replacement required.")

    # ---------- CHEMICAL STOCK ----------
    st.markdown("---")
    st.subheader("Chemical Stock Status (kg) and Value")

    if len(chem) == 0:
        st.info("No chemical stock data.")
    else:
        chem_disp = chem.copy()
        chem_disp["stock_value"] = chem_disp["qty"] * chem_disp["unit_cost"]
        st.dataframe(chem_disp)

        for _, row in chem.iterrows():
            name = row["name"]
            qty = float(row["qty"])
            st.write(f"**{name}: {qty:.1f} kg**")
            progress_value = qty / 200.0
            progress_value = max(0.0, min(progress_value, 1.0))
            st.progress(progress_value)

            if qty < 50:
                st.error(f"{name} below 50 kg: Reorder required.")
            elif qty < 80:
                st.warning(f"{name} approaching low stock, monitor.")


def page_add_reading():
    st.title("Daily RO Reading - Um Qasr Port")

    d = st.date_input("Date", date.today())
    tds = st.number_input("TDS (ppm)", min_value=0.0, step=0.1)
    ph = st.number_input("pH", min_value=0.0, max_value=14.0, step=0.01)
    cond = st.number_input("Conductivity (uS/cm)", min_value=0.0, step=1.0)
    flow = st.number_input("Flow (m3/hr)", min_value=0.0, step=0.1)
    prod = st.number_input("Production Today (m3)", min_value=0.0, step=0.1)

    st.markdown("### Maintenance / Notes")
    maint = st.text_area("Maintenance done today")
    notes = st.text_area("Notes / alarms / comments")

    if st.button("Save Daily Reading"):
        add_reading(str(d), tds, ph, cond, flow, prod, maint, notes)
        st.success("Daily RO reading saved successfully.")


def page_chemicals():
    st.title("Chemicals - Stock, Cost and Movements")

    chem_df = get_chem()
    move_df = get_chemical_movements()

    tab1, tab2, tab3 = st.tabs(["Stock & Cost", "Add IN / OUT", "Movements History"])

    # ---- STOCK & COST ----
    with tab1:
        st.subheader("Current Stock and Unit Cost")
        disp = chem_df.copy()
        disp["stock_value"] = disp["qty"] * disp["unit_cost"]
        st.dataframe(disp)

        st.markdown("### Update Unit Cost (per kg)")
        if len(chem_df) > 0:
            col1, col2, col3 = st.columns(3)
            with col1:
                chem_name = st.selectbox(
                    "Chemical",
                    chem_df["name"].tolist(),
                    key="chem_cost_name",
                )
            with col2:
                current_cost = float(
                    chem_df.loc[chem_df["name"] == chem_name, "unit_cost"].iloc[0]
                )
                new_cost = st.number_input(
                    "Unit cost (per kg)",
                    min_value=0.0,
                    value=current_cost,
                    step=0.1,
                    key="chem_unit_cost",
                )
            with col3:
                if st.button("Save Unit Cost"):
                    update_chemical_cost(chem_name, new_cost)
                    st.success(f"Unit cost updated for {chem_name}. Refresh to view.")

    # ---- ADD MOVEMENTS ----
    with tab2:
        st.subheader("Record Chemical IN / OUT")

        d = st.date_input("Date", date.today(), key="chem_date")
        if len(chem_df) > 0:
            name = st.selectbox(
                "Chemical",
                chem_df["name"].tolist(),
                key="chem_name_select",
            )
        else:
            name = st.text_input("Chemical name", key="chem_name_manual")

        movement_type = st.selectbox(
            "Movement Type", ["IN", "OUT"], key="chem_move_type"
        )
        qty = st.number_input(
            "Quantity (kg)", min_value=0.0, step=0.1, key="chem_qty"
        )
        remarks = st.text_input(
            "Remarks / reference (invoice, batch, etc.)",
            key="chem_remarks",
        )

        if st.button("Save Movement"):
            if not name:
                st.error("Please select or enter a chemical name.")
            elif qty <= 0:
                st.error("Quantity must be greater than 0.")
            else:
                record_chemical_movement(str(d), name, movement_type, qty, remarks)
                st.success(f"{movement_type} movement recorded for {name}.")

    # ---- HISTORY ----
    with tab3:
        st.subheader("All Chemical Movements")
        st.dataframe(move_df)


def page_cartridge():
    st.title("Cartridge Filter - DP Monitoring and Log")
    st.caption("DP = Pressure Before Filter - Pressure After Filter (0-10 bar)")

    st.subheader("Enter Pressure Readings")

    col1, col2 = st.columns(2)
    with col1:
        before = st.number_input(
            "Pressure BEFORE Filter (bar)", min_value=0.0, max_value=10.0, value=0.0, step=0.1
        )
    with col2:
        after = st.number_input(
            "Pressure AFTER Filter (bar)", min_value=0.0, max_value=10.0, value=0.0, step=0.1
        )

    if after > before:
        st.warning("After pressure cannot be higher than before. Adjusting AFTER to BEFORE.")
        after = before

    dp = before - after
    if dp < 0:
        dp = 0.0

    st.metric("Differential Pressure (DP)", f"{dp:.2f} bar")

    st.subheader("Differential Pressure Gauge (0-10 bar)")
    render_dp_gauge(dp)

    if dp < 1:
        st.success("DP Normal - Cartridge OK (DP < 1 bar).")
    elif dp <= 4:
        st.warning("DP Warning (1-4 bar) - monitor filter.")
    else:
        st.error("High DP > 4 bar - cartridge replacement required immediately.")

    st.subheader("Save Cartridge Record")
    d = st.date_input("Date", date.today(), key="cart_date")
    remarks = st.text_input("Remarks (change / inspection / cleaning)", key="cart_remarks")

    is_change = st.checkbox("Cartridge replaced during this visit?", key="cart_is_change")
    change_cost = 0.0
    if is_change:
        change_cost = st.number_input(
            "Replacement cost", min_value=0.0, step=1.0, value=0.0, key="cart_cost"
        )

    if st.button("Save Cartridge Log"):
        add_cartridge(str(d), dp, remarks, is_change=is_change, change_cost=change_cost)
        st.success("Cartridge filter reading saved successfully.")

    st.markdown("---")
    st.subheader("Cartridge Filter History")
    st.dataframe(get_cartridge())
# -----------------------------------------------------------
# CHUNK 6 - MONTHLY REPORT PAGES + MAIN
# -----------------------------------------------------------

def page_report():
    st.title("Monthly Report - Water Quality, Chemicals and Cost")

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
        df_chem_out = pd.DataFrame(
            columns=["d", "name", "movement_type", "qty", "remarks"]
        )

    # Cartridge records that month
    cart_df = get_cartridge()
    if len(cart_df) > 0:
        cart_df["month"] = cart_df["d"].dt.strftime("%Y-%m")
        df_cart_m = cart_df[cart_df["month"] == month]
    else:
        df_cart_m = pd.DataFrame(
            columns=["d", "dp", "remarks", "is_change", "change_cost"]
        )

    # ---- Water trend ----
    st.subheader("Water Quality Trend (TDS / pH / Conductivity)")
    fig, ax = plt.subplots(figsize=(5, 2.5))
    ax.plot(df_m["d"], df_m["tds"], label="TDS")
    ax.plot(df_m["d"], df_m["ph"], label="pH")
    ax.plot(df_m["d"], df_m["conductivity"], label="Cond")
    ax.legend()
    plt.xticks(rotation=45)
    st.pyplot(fig)

    # ---- Detail table ----
    st.subheader("Daily Details")
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
        ]
    )

    # ---- Chemical consumption summary ----
    st.subheader("Chemical Consumption (OUT) This Month")
    if len(df_chem_out) > 0:
        st.dataframe(df_chem_out)

        chem_df = get_chem()
        cost_map = {row["name"]: float(row["unit_cost"]) for _, row in chem_df.iterrows()}

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
        st.write("Chemical usage and cost summary:")
        st.dataframe(summary_df)
    else:
        st.info("No chemical OUT movements recorded this month.")
        total_chem_cost = 0.0

    # ---- Cartridge activity & cost ----
    st.subheader("Cartridge Filter Activity This Month")
    if len(df_cart_m) > 0:
        st.dataframe(df_cart_m)

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
    st.subheader("Monthly Consumables Cost Summary")
    total_consumable_cost = total_chem_cost + total_cart_cost
    cost_per_m3 = total_consumable_cost / total_prod if total_prod > 0 else 0.0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Chemical Cost", f"{total_chem_cost:.2f}")
    col2.metric("Cartridge Changes", num_changes)
    col3.metric("Cartridge Cost", f"{total_cart_cost:.2f}")
    col4.metric("Total Cost per m3", f"{cost_per_m3:.4f}")

    # ---- PDF Export ----
    if st.button("Export Monthly Water/Chem PDF"):
        file = create_pdf(month, df_m, df_chem_out, df_cart_m)
        with open(file, "rb") as f:
            st.download_button("Download PDF", f, file_name=file)
        st.success("Monthly PDF report generated.")


def page_maintenance_report():
    st.title("Maintenance Report - Daily Actions and Cartridge")

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
        df_cart_m = pd.DataFrame(
            columns=["d", "dp", "remarks", "is_change", "change_cost"]
        )

    st.subheader("Daily Maintenance Notes")
    if len(df_maint) > 0:
        st.dataframe(df_maint[["d", "maintenance", "notes"]])
    else:
        st.info("No maintenance notes recorded for this month.")

    st.subheader("Cartridge Filter DP & Actions")
    if len(df_cart_m) > 0:
        st.dataframe(df_cart_m)
    else:
        st.info("No cartridge entries for this month.")

    if st.button("Export Maintenance PDF"):
        file = create_maintenance_pdf(month, df_maint, df_cart_m)
        with open(file, "rb") as f:
            st.download_button("Download Maintenance PDF", f, file_name=file)
        st.success("Maintenance PDF generated.")


# ---------------------- MAIN ENTRY POINT ----------------------
def main():
    st.set_page_config(layout="wide", page_title="RO - Um Qasr Port")

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
        page_report()
    elif menu == "Maintenance Report":
        page_maintenance_report()


if __name__ == "__main__":
    init_db()
    main()
