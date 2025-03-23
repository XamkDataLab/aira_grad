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