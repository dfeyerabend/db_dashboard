# ============================================================
#
#   DEUTSCHE BAHN PERFORMANCE DASHBOARD
#   Tag 12 - Data Transformation Projekt
#
# ============================================================

import streamlit as st
import duckdb
import pandas as pd

# ============================================================
# KONFIGURATION
# ============================================================

st.set_page_config(
    page_title="Deutsche Bahn Dashboard",
    page_icon="🚆",
    layout="wide"
)

# data path
DATA_PATH = "./data/deutsche_bahn_data/monthly_processed_data/data-2024-10.parquet"

# DuckDB Connection wit streamlit integration
@st.cache_resource
def get_connection():
    con = duckdb.connect()
    # Create a named view pointing at the parquet file
    con.execute(f"""
        CREATE OR REPLACE VIEW data_table AS
        SELECT * FROM read_parquet('{DATA_PATH}')
    """)
    return con

con = get_connection()

# ============================================================
# TITEL
# ============================================================

st.title("🚆 Deutsche Bahn Performance Dashboard")
st.markdown("**Datenquelle:** Oktober 2024 | ~2 Millionen Zugfahrten")
st.markdown("---")

# ============================================================
# TEST: Daten laden
# ============================================================

try:
    test = con.execute(f"SELECT COUNT(*) FROM data_table").fetchone()
    st.success(f"✅ Daten geladen: {test[0]:,} Zeilen")
except Exception as e:
    st.error(f"❌ Fehler beim Laden: {e}")
    st.stop()

# ============================================================
# KPI BERECHNUNG
# ============================================================

@st.cache_data
def get_kpis():
    """Calculate Main KPIs"""
    result = con.execute("""
        SELECT
            COUNT(*) AS total_rides,
            ROUND(AVG(delay_in_min), 2) AS avg_delay,
            ROUND(SUM(CASE WHEN delay_in_min <= 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_pct,
            ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS canceled_pct,
            MIN(time) AS start_date,
            MAX(time) AS end_date
        FROM data_table
        WHERE delay_in_min IS NOT NULL
    """).fetchone()

    return {
        'total_rides': result[0],
        'avg_delay': result[1],
        'on_time_pct': result[2],
        'canceled_pct': result[3],
        'start_date': result[4],
        'end_date': result[5]
    }

kpis = get_kpis()

# ============================================================
# KPI CARDS ANZEIGEN
# ============================================================

st.subheader("📊 Key Performance Indicators")

# 4 columns for KPIs
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total number of rides",
        value=f"{kpis['total_rides']:,}"
    )

with col2:
    st.metric(
        label="Average delays",
        value=f"{kpis['avg_delay']} min"
    )

with col3:
    st.metric(
        label="On time (≤5 min)",
        value=f"{kpis['on_time_pct']}%"
    )

with col4:
    st.metric(
        label="Canceled rides",
        value=f"{kpis['canceled_pct']}%"
    )

st.markdown("---")

# ============================================================
# RUSH HOUR ANALYSE
# ============================================================

@st.cache_data
def get_rush_hour_stats():
    """Vergleicht Rush Hour mit normalen Zeiten"""
    result = con.execute(f"""
    SELECT
        CASE
            WHEN HOUR(time) BETWEEN 7 AND 9 THEN 'Morning Rush (7-9)'
            WHEN HOUR(time) BETWEEN 16 AND 19 THEN 'Evening Rush (16-19)'
            ELSE 'Normal'
        END as zeitfenster,
        COUNT(*) as fahrten,
        ROUND(AVG(delay_in_min), 2) as avg_delay,
        ROUND(SUM(CASE WHEN delay_in_min > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as verspaetet_pct,
        ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as canceled_pct
    FROM '{DATA_PATH}'
    WHERE delay_in_min IS NOT NULL
    GROUP BY 1
    ORDER BY avg_delay DESC
    """).fetchdf()

    return result

rush_hour_df = get_rush_hour_stats()

st.subheader("🕐 Rush Hour Analysis")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Average delay by Time of Day**")
    st.bar_chart(
        rush_hour_df.set_index('zeitfenster')['avg_delay'],
        color='#FF6B6B'
    )

with col2:
    st.markdown("**Detailed statistics**")
    st.dataframe(
        rush_hour_df,
        hide_index=True,
        use_container_width=True
    )

# Business Insight Box
worst_time = rush_hour_df.iloc[0]['zeitfenster']
worst_delay = rush_hour_df.iloc[0]['avg_delay']

st.info(f"""
**💡 Business Insights:**
{worst_time} has the highest average delay times with ({worst_delay} min).
Recommendation: Plan additional capacities for the respective time window.
""")

st.markdown("---")

