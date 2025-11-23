# UMQASR-RO – RO Monitoring System (NFM / NPS Cloud)

This repository contains a Streamlit application for monitoring the RO unit at
Um Qasr Port (capacity 10 m³/hr) for Nile Facility Management (NFM) / Nile
Projects Service (NPS).

The app is designed for daily operation follow-up and monthly reporting of RO
performance and chemical consumption.

---

## Main Features

- **Daily Readings**
  - TDS (ppm)
  - pH
  - Conductivity (µS/cm)
  - Flow (m³/hr)
  - Daily production (m³)
  - Maintenance notes and alarms

- **Cartridge Filter Monitoring**
  - Differential Pressure (DP = before – after, 0–10 bar)
  - Visual gauge (green / orange / red zones)
  - Log of checks and cartridge changes
  - Cost per cartridge change

- **Chemical Management**
  - Stock for Chlorine, HCL, and BC (kg)
  - IN / OUT movements (deliveries and consumption)
  - Unit cost per kg
  - Monthly chemical usage and cost per m³

- **Reports**
  - Monthly water quality + chemical cost PDF
  - Monthly maintenance + cartridge actions PDF
  - KPIs: compliance %, production, cost per m³

---

## Technology

- Python
- Streamlit
- Pandas, NumPy
- Matplotlib
- FPDF
- SQLAlchemy
- PostgreSQL (Neon) or SQLite fallback

---

## Database Configuration

The app uses SQLAlchemy and reads the database connection string from the
`DB_URL` environment variable.

**Example (Neon PostgreSQL):**

```text
DB_URL="postgresql://USER:PASSWORD@HOST:PORT/DBNAME?sslmode=require&channel_binding=require"
