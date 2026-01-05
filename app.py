import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector

# sets browser tab title and page layout 
st.set_page_config(page_title="Snowflake Demo", layout="wide")

# Connects once and reuse (faster + cheaper + cleaner)
@st.cache_resource
def get_connection():
    load_dotenv()
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )
# helper function
def run_query(conn, sql: str) -> pd.DataFrame:
    """Run SQL in Snowflake and return results as a DataFrame."""
    return pd.read_sql(sql, conn)

# helper function
def get_context(conn) -> dict:
    """Ask Snowflake: what role/warehouse/database/schema is this session using?"""
    sql = "SELECT CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS warehouse, CURRENT_DATABASE() AS database, CURRENT_SCHEMA() AS schema;"
    df = pd.read_sql(sql, conn)
    return df.iloc[0].to_dict()

st.title("❄️ Snowflake + Streamlit Demo")
st.write("This app connects to Snowflake, runs SQL queries, and displays results.")

conn = get_connection()

# --- Upgrade #1: Connection Status (makes demo more professional) ---
with st.expander("✅ Connection status (current Snowflake session context)", expanded=True):
    try:
        ctx = get_context(conn)
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Role", ctx["ROLE"])
        col2.metric("Warehouse", ctx["WAREHOUSE"])
        col3.metric("Database", ctx["DATABASE"])
        col4.metric("Schema", ctx["SCHEMA"])
    except Exception as e:
        st.warning("Could not fetch Snowflake session context.")
        st.code(str(e))

st.divider()

# --- Query area ---
# how many menu items does each food truck brand have
default_query = """
SELECT
    truck_brand_name,
    COUNT(*) AS item_count
FROM menu
GROUP BY truck_brand_name
ORDER BY item_count DESC;
"""

query = st.text_area("SQL Query", value=default_query, height=180)

# --- Upgrade: Safety toggle ---
limit_results = st.checkbox("Limit results to 200 rows", value=True)

if st.button("Run query"):
    try:
        sql_to_run = query.strip()

        # Add a LIMIT if user wants safety and query doesn't already have LIMIT
        if limit_results and "limit" not in sql_to_run.lower():
            sql_to_run = sql_to_run.rstrip(";") + "\nLIMIT 200;"

        df = run_query(conn, sql_to_run)

        st.success("Query ran successfully!")
        st.dataframe(df, use_container_width=True)

        # --- Upgrade #2: Chart (only if the right columns exist) ---
        if "ITEM_COUNT" in df.columns and "TRUCK_BRAND_NAME" in df.columns:
            st.subheader("📊 Items per brand (bar chart)")
            chart_df = df[["TRUCK_BRAND_NAME", "ITEM_COUNT"]].set_index("TRUCK_BRAND_NAME")
            st.bar_chart(chart_df)

    except Exception as e:
        st.error("Error running query")
        st.code(str(e))

