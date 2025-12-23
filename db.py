from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import create_engine, text, QueuePool

load_dotenv()
_engine = None

def get_engine():
    """Create SQLAlchemy engine with connection pooling"""
    global _engine
    if _engine is None:
        conn_string = "postgresql://{user}:{password}@{host}:{port}/{database}".format(
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            database=os.getenv("POSTGRES_DATABASE")
        )
        _engine = create_engine(
            conn_string,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
            connect_args={
                "connect_timeout": 10,
                "options": "-c statement_timeout=30000"
            }
        )
    return _engine

def execute_query(query, params=None, max_retries=3):
    """Execute a SQL query with retry logic and connection management"""
    for attempt in range(max_retries):
        try:
            engine = get_engine()
            with engine.connect() as conn:
                df = pd.read_sql_query(text(query), conn, params=params, chunksize=10000)
                if hasattr(df, '__iter__'):
                    df = pd.concat(df, ignore_index=True)
            return df, None
        except SQLAlchemyError as e:
            if attempt == max_retries - 1:
                return pd.DataFrame(), f"Database error after {max_retries} attempts: {e}"
            if "connection" in str(e).lower():
                global _engine
                _engine = None
        except Exception as e:
            return pd.DataFrame(), f"Unexpected error: {e}"
    return pd.DataFrame(), "Max retries exceeded"
