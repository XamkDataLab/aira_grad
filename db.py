import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def get_engine():
    """Create SQLAlchemy engine using Streamlit secrets"""
    conn_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
        user=st.secrets["postgres"]["user"],
        password=st.secrets["postgres"]["password"],
        host=st.secrets["postgres"]["host"],
        port=st.secrets["postgres"]["port"],
        database=st.secrets["postgres"]["database"]
    )
    return create_engine(conn_string)

def execute_query(query, params=None):
    """Execute a SQL query and return results as DataFrame using SQLAlchemy"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            df = pd.read_sql_query(text(query), conn, params=params)
        return df
    except SQLAlchemyError as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()
    
def get_filter_options():
    """Retrieve distinct values for event_type and hake from the database."""
    event_query = "SELECT DISTINCT event_type FROM tilanteet"
    hake_query = "SELECT DISTINCT hake FROM tilanteet"
    df_event = execute_query(event_query)
    df_hake = execute_query(hake_query)
    
    # Create options with an "All" choice
    event_types = ["All"] + sorted(df_event["event_type"].dropna().tolist())
    hake_values = ["All"] + sorted(df_hake["hake"].dropna().tolist())
    return event_types, hake_values

def load_data(selected_event_type="All", selected_hake="All"):
    """
    Load data from the database with filters applied in the SQL query.
    The base query selects municipality, timestamp, event_type, and hake.
    """
    query = "SELECT municipality, timestamp, event_type, hake FROM tilanteet WHERE 1=1"
    params = {}
    if selected_event_type != "All":
        query += " AND event_type = :event_type"
        params["event_type"] = selected_event_type
    if selected_hake != "All":
        query += " AND hake = :hake"
        params["hake"] = selected_hake
    return execute_query(query, params=params)