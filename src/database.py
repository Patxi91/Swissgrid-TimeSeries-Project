import psycopg2
import os

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            host="localhost",
            port=os.getenv("POSTGRES_PORT", "5431"),
            database=os.getenv("POSTGRES_DB", "timeseries_db"),
            user=os.getenv("POSTGRES_USER", "swissgrid"),
            password=os.getenv("POSTGRES_PASSWORD", "swissgrid1234")
        )
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None

