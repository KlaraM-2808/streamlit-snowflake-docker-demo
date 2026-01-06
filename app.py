import os
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import ProgrammingError

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


def _reconnect_and_retry(sql: str) -> pd.DataFrame:
    """
    Clear the cached Snowflake connection, reconnect, and rerun the SQL.
    This is used when Snowflake reports an expired session/token.
    """
    st.cache_resource.clear()
    fresh_conn = get_connection()
    return pd.read_sql(sql, fresh_conn)


# helper function
def run_query(conn, sql: str) -> pd.DataFrame:
    """Run SQL in Snowflake and return results as a DataFrame."""
    try:
        return pd.read_sql(sql, conn)

    except ProgrammingError as e:
        # 390114 is the Snowflake "Authentication token has expired" style error
        if getattr(e, "errno", None) == 390114:
            st.warning("Snowflake session expired — reconnecting…")
            return _reconnect_and_retry(sql)
        raise


# helper function
def get_context(conn) -> dict:
    """Ask Snowflake: what role/warehouse/database/schema is this session using?"""
    sql = (
        "SELECT CURRENT_ROLE() AS role, CURRENT_WAREHOUSE() AS warehouse, "
        "CURRENT_DATABASE() AS database, CURRENT_SCHEMA() AS schema;"
    )
    try:
        df = pd.read_sql(sql, conn)
        return df.iloc[0].to_dict()

    except ProgrammingError as e:
        if getattr(e, "errno", None) == 390114:
            st.warning("Snowflake session expired — reconnecting…")
            df = _reconnect_and_retry(sql)
            return df.iloc[0].to_dict()
        raise


st.title("❄️ Snowflake + Streamlit Demo")
st.write("This app connects to Snowflake, runs SQL queries, and displays results.")

conn = get_connection()

# --- Upgrade #1: Connection Status (makes demo more professional) ---
with st.expander("✅ Connection status (current Snowflake session context)", expanded=True):
    try:
        ctx = get_context(conn)
        col1, col2, col3, col4 = st.columns(4)

        # Your SQL aliases are lowercase, but Snowflake returns uppercase column names in pandas
        col1.metric("Role", ctx.get("ROLE") or ctx.get("role"))
        col2.metric("Warehouse", ctx.get("WAREHOUSE") or ctx.get("warehouse"))
        col3.metric("Database", ctx.get("DATABASE") or ctx.get("database"))
        col4.metric("Schema", ctx.get("SCHEMA") or ctx.get("schema"))

    except Exception as e:
        st.warning("Could not fetch Snowflake session context.")
        st.code(str(e))

st.divider()

# --- Query area ---
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


