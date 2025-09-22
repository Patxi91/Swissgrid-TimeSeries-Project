import psycopg2
import os

def test_db_connection():
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("POSTGRES_DB", "timeseries_db"),
            user=os.getenv("POSTGRES_USER", "swissgrid"),
            password=os.getenv("POSTGRES_PASSWORD", "swissgrid1234"),
            host=os.getenv("POSTGRES_HOST", "db"),
            port=os.getenv("POSTGRES_PORT", "5432")
        )
        assert conn is not None
    finally:
        if conn:
            conn.close()
