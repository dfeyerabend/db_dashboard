# ============================================================

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# ============================================================
# Configurations
# ============================================================

st.set_page_config(
    page_title="Deutsche Bahn Dashboard",
    page_icon="🚆",
    layout="wide"
)

# ============================================================
# TITEL & DATA SELECTION
# ============================================================

st.title("🚆 Deutsche Bahn Performance Dashboard")

# Year and Month selection
col1, col2 = st.columns([1, 3])

with col1:
    selected_year = st.selectbox(
        "Select Year:",
        options=[2024, 2023, 2022],  # Add more years as they become available
        index=0  # Default to 2024
    )

with col2:
    selected_month = st.selectbox(
        "Select Month:",
        options=[
            ("January", 1), ("February", 2), ("March", 3), ("April", 4),
            ("May", 5), ("June", 6), ("July", 7), ("August", 8),
            ("September", 9), ("October", 10), ("November", 11), ("December", 12)
        ],
        format_func=lambda x: x[0],  # Display month name
        index=9  # Default to October (index 9)
    )

# Construct data path based on selection
month_num = selected_month[1]
DATA_PATH = f"https://huggingface.co/datasets/piebro/deutsche-bahn-data/resolve/main/monthly_processed_data/data-{selected_year}-{month_num:02d}.parquet"

st.markdown(f"**Data source:** {selected_month[0]} {selected_year} | ~2 million train services")
st.markdown("---")


# ============================================================
# DUCKDB CONNECTION
# ============================================================

# DuckDB Connection with streamlit integration
@st.cache_resource
def get_connection():
    con = duckdb.connect()
    # Install and load HTTPFS extension for reading from URLs
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    return con


con = get_connection()

# Create view based on selected data
# Note: We recreate the view each time the selection changes
try:
    con.execute(f"""
        CREATE OR REPLACE VIEW data_table AS
        SELECT * FROM read_parquet('{DATA_PATH}')
    """)
except Exception as e:
    st.error(f"❌ Could not load data for {selected_month[0]} {selected_year}")
    st.error(f"Error: {e}")
    st.info("This dataset may not be available yet. Please try a different month/year combination.")
    st.stop()

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
def get_kpis(_year, _month):
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

kpis = get_kpis(selected_year, month_num)

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
def get_rush_hour_stats(_year, _month):
    """Vergleicht Rush Hour mit normalen Zeiten"""
    result = con.execute(f"""
    SELECT
        CASE
            WHEN HOUR(time) BETWEEN 7 AND 9 THEN 'Morning Rush (7-9)'
            WHEN HOUR(time) BETWEEN 16 AND 19 THEN 'Evening Rush (16-19)'
            ELSE 'Normal'
        END as time_window,
        COUNT(*) as total_trips,
        ROUND(AVG(delay_in_min), 2) as avg_delay,
        ROUND(SUM(CASE WHEN delay_in_min > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delayed_pct,
        ROUND(SUM(CASE WHEN is_canceled THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as canceled_pct
    FROM data_table
    WHERE delay_in_min IS NOT NULL
    GROUP BY 1
    ORDER BY avg_delay DESC
    """).fetchdf()

    return result

rush_hour_df = get_rush_hour_stats(selected_year, month_num)

st.subheader("🕐 Rush Hour Analysis")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Average delay by Time of Day**")
    st.bar_chart(
        rush_hour_df.set_index('time_window')['avg_delay'],
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
worst_time = rush_hour_df.iloc[0]['time_window']
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
def get_weekday_stats(_year, _month):
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

weekday_df = get_weekday_stats(selected_year, month_num)

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
# DELAY DISTRIBUTION ANALYSIS
# ============================================================

@st.cache_data
def get_delay_distribution():
    """Analyzes delay distribution across different buckets"""
    result = con.execute("""
                         SELECT CASE
                                    WHEN delay_in_min <= 0 THEN '0. Early/On-Time'
                                    WHEN delay_in_min <= 5 THEN '1. 1-5 min'
                                    WHEN delay_in_min <= 15 THEN '2. 6-15 min'
                                    WHEN delay_in_min <= 30 THEN '3. 16-30 min'
                                    WHEN delay_in_min <= 60 THEN '4. 31-60 min'
                                    ELSE '5. 60+ min'
                                    END                                            as delay_bucket,
                                COUNT(*)                                           as trip_count,
                                ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
                         FROM data_table
                         WHERE delay_in_min IS NOT NULL
                         GROUP BY 1
                         ORDER BY 1
                         """).fetchdf()

    # Clean up the labels (remove the ordering prefix)
    result['delay_bucket'] = result['delay_bucket'].str.replace(r'^\d+\.\s*', '', regex=True)

    return result

st.subheader("📊 Delay Distribution Analysis")

delay_dist_df = get_delay_distribution()

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("**How are delays distributed?**")

    # Create a more polished bar chart
    import plotly.express as px

    fig = px.bar(
        delay_dist_df,
        x='delay_bucket',
        y='trip_count',
        text='percentage',
        color='percentage',
        color_continuous_scale='RdYlGn_r',  # Red for high delays, green for low
        labels={
            'delay_bucket': 'Delay Category',
            'trip_count': 'Number of Trips',
            'percentage': 'Percentage (%)'
        }
    )

    # Customize the chart
    fig.update_traces(
        texttemplate='%{text:.1f}%',
        textposition='outside'
    )

    fig.update_layout(
        showlegend=False,
        height=400,
        yaxis_title="Number of Trips",
        xaxis_title="Delay Category"
    )

    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("**Distribution Summary**")
    st.dataframe(
        delay_dist_df,
        hide_index=True,
        use_container_width=True,
        column_config={
            "delay_bucket": "Delay Range",
            "trip_count": st.column_config.NumberColumn(
                "Trips",
                format="%d"
            ),
            "percentage": st.column_config.NumberColumn(
                "Share",
                format="%.1f%%"
            )
        }
    )

# Business Insights
on_time_pct = delay_dist_df[delay_dist_df['delay_bucket'].isin(['Early/On-Time', '1-5 min'])]['percentage'].sum()
severe_delay_pct = delay_dist_df[delay_dist_df['delay_bucket'].isin(['31-60 min', '60+ min'])]['percentage'].sum()

st.info(f"""
**💡 Business Insights:**
- **{on_time_pct:.1f}%** of trains are on-time or have minimal delays (≤5 min)
- **{severe_delay_pct:.1f}%** experience severe delays (>30 min)
- The distribution shows a **long tail** - while most trains run smoothly, a small percentage with severe delays disproportionately impacts customer satisfaction
- **Recommendation:** Focus improvement efforts on the {severe_delay_pct:.1f}% severe delay cases for maximum customer impact
""")

st.markdown("---")

# ============================================================
# ZUGTYP ANALYSE MIT FILTER
# ============================================================

@st.cache_data
def get_train_types(_year, _month):

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
def get_train_type_stats(selected_types, _year, _month):
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
all_train_types = get_train_types(selected_year, month_num)

# Create filters
selected_types = st.multiselect(
    "Choose train types to compare:",
    options=all_train_types,
    default=['ICE', 'IC', 'RE', 'RB', 'S']  # Standard-Choice
)

# Only show if at least one type was selected
if selected_types:
    train_type_df = get_train_type_stats(selected_types, selected_year, month_num)

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