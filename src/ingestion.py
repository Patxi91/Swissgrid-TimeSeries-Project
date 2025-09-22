import pandas as pd
from database import get_db_connection
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
import locale

def ingest_data(file_path):
    """
    Ingests data from a CSV file into the PostgreSQL database.
    This script is now idempotent and will create the table and hypertable
    if they do not already exist.
    """
    conn = get_db_connection()
    if not conn:
        print("Could not connect to the database. Exiting ingestion.")
        return

    try:
        with conn.cursor() as cursor:
            # 1. Drop the table if it exists to ensure the schema is correct, then create it.
            #    This is for initial setup and schema changes.
            print("Ensuring 'swissgrid_frequency_data' table has correct schema...")
            cursor.execute("DROP TABLE IF EXISTS swissgrid_frequency_data CASCADE;")
            conn.commit()
            create_table_sql = """
            CREATE TABLE swissgrid_frequency_data (
                timestamp TIMESTAMPTZ NOT NULL PRIMARY KEY,
                frequency FLOAT NOT NULL
            );
            """
            cursor.execute(create_table_sql)
            conn.commit()
            print("Table 'swissgrid_frequency_data' is ready with the correct schema.")

            # 2. Check if the table is already a hypertable and convert if not
            cursor.execute("""
                SELECT
                    COUNT(*)
                FROM
                    timescaledb_information.hypertables
                WHERE
                    hypertable_name = 'swissgrid_frequency_data';
            """)
            is_hypertable = cursor.fetchone()[0] > 0

            if not is_hypertable:
                print("Converting 'swissgrid_frequency_data' to a hypertable...")
                create_hypertable_sql = "SELECT create_hypertable('swissgrid_frequency_data', 'timestamp');"
                cursor.execute(create_hypertable_sql)
                conn.commit()
                print("Hypertable created successfully.")
            else:
                print("'swissgrid_frequency_data' is already a hypertable.")

        # 3. Proceed with data ingestion
        print(f"Reading data from {file_path}...")
        df = pd.read_csv(
            file_path, 
            delimiter=';' # Specify the delimiter
        )

        # Set locale for date parsing as '%a' is locale-dependent
        try:
            locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
        except locale.Error:
            try:
                locale.setlocale(locale.LC_TIME, 'de_DE.utf8')
            except locale.Error:
                try:
                    locale.setlocale(locale.LC_TIME, 'German')
                except locale.Error:
                    print("Warning: Could not set locale for date parsing. This may cause an error.")

        # Explicitly convert the 'Datum Zeit' column to a proper timestamp format
        # The format string matches the German locale format "Sa. 01.05.21 00:00:30"
        df['Datum Zeit'] = pd.to_datetime(df['Datum Zeit'], format='%a. %d.%m.%y %H:%M:%S', errors='coerce')
        
        # Rename columns to match the database schema
        df.rename(columns={'Datum Zeit': 'timestamp', 'A:f_soll_aktiv [Hz]': 'frequency'}, inplace=True)
        
        # Drop rows with NaT (Not a Time) from failed parsing
        df.dropna(subset=['timestamp'], inplace=True)

        print(f"Ingesting {len(df)} rows into the database...")
        
        # Use execute_values for fast bulk insertion
        data_to_insert = [tuple(row) for row in df[['timestamp', 'frequency']].to_numpy()]
        
        cursor = conn.cursor()
        query = "INSERT INTO swissgrid_frequency_data (timestamp, frequency) VALUES %s ON CONFLICT (timestamp) DO NOTHING;"
        execute_values(cursor, query, data_to_insert)

        conn.commit()
        print("Data ingestion complete.")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        conn.rollback()
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    # Pathlib to handle file paths.
    data_file_path = Path(__file__).parent.parent / "data" / "Sollfrequenz.csv"
    # Run inestion
    ingest_data(data_file_path)
