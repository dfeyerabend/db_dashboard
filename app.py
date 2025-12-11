# ============================================================
#
#   DEUTSCHE BAHN PERFORMANCE DASHBOARD
#
# ============================================================

import streamlit as st
import duckdb
import pandas as pd

# ============================================================
# Configurations
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
st.markdown("**Data source:** October 2024 | ~2 million train services")
st.markdown("---")

# ============================================================
# TEST: Daten laden
# ============================================================

try:
    test = con.execute(f"SELECT COUNT(*) FROM data_table").fetchone()
    st.success(f"✅ Loaded data successfully: {test[0]:,} Rows")
except Exception as e:
    st.error(f"❌ Error while loading: {e}")
    st.stop()

# ============================================================
# KPI BERECHNUNG
# ============================================================

@st.cache_data
def get_kpis():
    """Calculate Main KPIs"""
    result = con.execute("""
        SELECT
            COUNT(*) AS total_trips,
            ROUND(AVG(delay_in_min), 2) AS avg_delay,
            ROUND(SUM(CASE WHEN delay_in_min <= 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS punctuality_pct,
            ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS canceled_pct,
            MIN(time) AS start_date,
            MAX(time) AS end_date
        FROM data_table
        WHERE delay_in_min IS NOT NULL
    """).fetchone()

    return {
        'total_trips': result[0],
        'avg_delay': result[1],
        'punctuality_pct': result[2],
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
        value=f"{kpis['total_trips']:,}"
    )

with col2:
    st.metric(
        label="Average delays",
        value=f"{kpis['avg_delay']} min"
    )

with col3:
    st.metric(
        label="On time (≤5 min)",
        value=f"{kpis['punctuality_pct']}%"
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

# ============================================================
# WOCHENTAG ANALYSE
# ============================================================

@st.cache_data
def get_weekday_stats():
    """Analysiert Verspätungen nach Wochentag"""
    result = con.execute(f"""
    SELECT
        CASE DAYOFWEEK(time)
            WHEN 0 THEN 'Sunday'
            WHEN 1 THEN 'Monday'
            WHEN 2 THEN 'Tuesday'
            WHEN 3 THEN 'Wednesday'
            WHEN 4 THEN 'Thursday'
            WHEN 5 THEN 'Friday'
            WHEN 6 THEN 'Saturday'
            ELSE 'Unknown'
        END as weekday,
        DAYOFWEEK(time) as day_number,
        COUNT(*) as total_trips,
        ROUND(AVG(delay_in_min), 2) as avg_delay,
        ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as canceled_pct
    FROM data_table
    WHERE delay_in_min IS NOT NULL
    GROUP BY DAYOFWEEK(time)
    ORDER BY DAYOFWEEK(time)
    """).fetchdf()

    return result

weekday_df = get_weekday_stats()

# 4.2 Show Weekday charts

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Delays by Day of the Week**")
    st.bar_chart(
        weekday_df.set_index('weekday')['avg_delay'],
        color='#4ECDC4'
    )

with col2:
    st.markdown("**Cancellations by Day of the Week**")
    st.bar_chart(
        weekday_df.set_index('weekday')['canceled_pct'],
        color='#FFE66D'
    )

# Best and worst day
best_day = weekday_df.loc[weekday_df['avg_delay'].idxmin()]
worst_day = weekday_df.loc[weekday_df['avg_delay'].idxmax()]

col1, col2 = st.columns(2)
with col1:
    st.success(f"✅ **Best day:** {best_day['weekday']} ({best_day['avg_delay']} min)")
with col2:
    st.error(f"❌ **Worst day:** {worst_day['weekday']} ({worst_day['avg_delay']} min)")

st.markdown("---")

# ============================================================
# ZUGTYP ANALYSE MIT FILTER
# ============================================================

@st.cache_data
def get_train_types():

    """get all available train names"""
    result = con.execute(f"""
        SELECT 
            DISTINCT train_type
        FROM data_table
        WHERE train_type IS NOT NULL
        ORDER BY train_type
    """).fetchdf()

    return result['train_type'].tolist()

@st.cache_data
def get_train_type_stats(selected_types):
    """Analyzes Performance according to train type"""
    # Converts list to SQL IN clause
    types_str = ", ".join([f"'{t}'" for t in selected_types])

    result = con.execute(f"""
    SELECT
        train_type,
        COUNT(*) as total_trips,
        ROUND(AVG(delay_in_min), 2) as avg_delay,
        ROUND(SUM(CASE WHEN delay_in_min <= 5 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as punctuality_pct,
        ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as canceled_pct
    FROM data_table
    WHERE train_type IN ({types_str})
      AND delay_in_min IS NOT NULL
    GROUP BY train_type
    ORDER BY avg_delay DESC
    """).fetchdf()

    return result

# 5.2 Filter and show chart

st.subheader("🚄 Train type comparison")

# Get train types
all_train_types = get_train_types()

# Create filters
selected_types = st.multiselect(
    "Choose train types to compare:",
    options=all_train_types,
    default=['ICE', 'IC', 'RE', 'RB', 'S']  # Standard-Choice
)

# Only show if at least one type was selected
if selected_types:
    train_type_df = get_train_type_stats(selected_types)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Average delays**")
        st.bar_chart(
            train_type_df.set_index('train_type')['avg_delay'],
            color='#9B59B6'
        )

    with col2:
        st.markdown("**Punctuality Rate**")
        st.bar_chart(
            train_type_df.set_index('train_type')['punctuality_pct'],
            color='#2ECC71'
        )

    # Detaillierte Tabelle
    st.markdown("**Detailed Statistics:**")
    st.dataframe(
        train_type_df,
        hide_index=True,
        use_container_width=True
    )
else:
    st.warning("⚠️ Please select at least one train type.")

st.markdown("---")

# Footer

st.markdown("""
### 📝 About this Dashboard

**Data source:** Deutsche Bahn API via HuggingFace
**Creation date:** October 2024
**Data volume:** ~2 million train services  

**Created by:** Dennis Feyerabend
**Date:** December 2025
**Technologies used:** Python, DuckDB, Streamlit, Railway

---

*This dashboard was developed as part of the Big Data module at Morphos GmbH.*
""")

with st.expander("🔍 Show raw data"):
    sample = con.execute(f"SELECT * FROM data_table LIMIT 100").fetchdf()
    st.dataframe(sample)