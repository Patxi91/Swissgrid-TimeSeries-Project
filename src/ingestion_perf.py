import psycopg2
from database import get_db_connection
from pathlib import Path
import io
import time
import sys
import re
from multiprocessing import Pool, cpu_count
from typing import Optional

# Attempt to import the high-performance C library, ciso8601.
try:
    import ciso8601
except ImportError:
    print("Warning: ciso8601 library not found. Falling back to slower native datetime parsing.")
    print("For a major performance boost, install it with: pip install ciso8601")
    ciso8601 = None


def transform_timestamp_fast(line: str) -> Optional[str]:
    """
    Transforms a single CSV line's timestamp and frequency into a clean format.
    This function is designed to be executed in parallel by multiple processes.
    
    Returns:
        A formatted string for a valid line, or None for an invalid line.
    """
    try:
        cols = line.rstrip("\n").split(";")
        if len(cols) < 2:
            return None # Skip invalid lines
        
        raw_ts = cols[0].strip().strip('"')
        raw_freq = cols[1].strip().strip('"').replace(",", ".")
        
        # Fast check to skip non-numeric lines
        float(raw_freq)
        
        # Use a more robust regular expression for parsing.
        match = re.search(r'(\d{2})\.(\d{2})\.(\d{2})\s+(\d{2}:\d{2}:\d{2})', raw_ts)
        if not match:
            return None
        
        day, month, year, time_part = match.groups()
        full_year = f"20{year}"
        iso_string = f"{full_year}-{month}-{day} {time_part}"

        # If ciso8601 is available, use it for a speed boost
        if ciso8601:
            dt_obj = ciso8601.parse_datetime(iso_string)
            return f"{dt_obj.strftime('%Y-%m-%d %H:%M:%S')},{raw_freq}\n"
        else:
            return f"{iso_string},{raw_freq}\n"
    except (ValueError, IndexError):
        return None
    except Exception:
        return None


def ensure_table_and_hypertable(conn, table_name: str, clear_data: bool = False):
    """
    Ensures the specified table and hypertable exist.
    This is an idempotent operation that does not drop the table if it exists.
    
    Args:
        conn: The database connection object.
        table_name (str): The name of the table to create/check.
        clear_data (bool): If True, truncates the table data.
    """
    with conn.cursor() as cur:
        # Step 1: Check if the table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = %s
            );
        """, (table_name,))
        table_exists = cur.fetchone()[0]

        if not table_exists:
            print(f"Creating '{table_name}' table...")
            create_table_sql = f"""
            CREATE TABLE {table_name} (
                timestamp TIMESTAMPTZ NOT NULL PRIMARY KEY,
                frequency DOUBLE PRECISION NOT NULL
            );
            """
            cur.execute(create_table_sql)
            conn.commit()
            print(f"Table '{table_name}' created.")
        else:
            print(f"Table '{table_name}' already exists.")

        # Step 2: Check if the table is a hypertable and convert if not
        cur.execute("""
            SELECT COUNT(*) FROM timescaledb_information.hypertables
            WHERE hypertable_name = %s;
        """, (table_name,))
        is_hypertable = cur.fetchone()[0] > 0
        if not is_hypertable:
            print(f"Converting '{table_name}' to a hypertable...")
            try:
                cur.execute(f"SELECT create_hypertable('{table_name}', 'timestamp', if_not_exists => TRUE);")
            except psycopg2.Error:
                cur.execute(f"SELECT create_hypertable('{table_name}', 'timestamp');")
            conn.commit()
            print("Hypertable created.")
        else:
            print("Hypertable already exists.")
            
        # Step 3: Optional - Truncate the table data if requested.
        if clear_data:
            print(f"Truncating existing data from '{table_name}'...")
            cur.execute(f"TRUNCATE TABLE {table_name};")
            conn.commit()
            print("Table data truncated.")


def get_file_line_count(file_path: Path) -> int:
    """Quickly counts the number of lines in a file, excluding the header."""
    try:
        with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
            # Subtract 1 to account for the header row
            return sum(1 for line in f) - 1
    except FileNotFoundError:
        return 0


def ingest_parallel(input_path: Path, table_name: str):
    """
    Ingests data into the specified table by parallelizing the transformation step.
    
    Args:
        input_path (Path): The path to the input CSV file.
        table_name (str): The name of the database table to ingest into.
    """
    conn = get_db_connection()
    if not conn:
        print("Could not obtain DB connection. Exiting.")
        return

    try:
        # Step 1: Ensure the database is ready for ingestion
        start_setup = time.time()
        # Always clear data for a fresh ingest, as requested by the user.
        ensure_table_and_hypertable(conn, table_name, clear_data=True)
        elapsed_setup = time.time() - start_setup
        print(f"Database setup finished in {elapsed_setup:.2f} seconds.")
        
        # Step 2: Read the raw data from the file
        print(f"Reading raw data from {input_path}...")
        with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
            lines = f.readlines()[1:] # Skip the header
        total_rows = len(lines)
        print(f"Read {total_rows:,} raw lines.")
        
        if total_rows == 0:
            print("No data rows found in the input file.")
            conn.close()
            return
        
        # Step 3: Use a process pool to parallelize the transformation with progress tracking
        num_cpus = cpu_count()
        print(f"Using a pool of {num_cpus} processes to transform data...")
        start_transform = time.time()
        
        transformed_lines = []
        with Pool(processes=num_cpus) as pool:
            # Use imap_unordered for an iterator that allows progress tracking
            # chunksize determines how many lines are sent to a worker at a time
            results = pool.imap_unordered(transform_timestamp_fast, lines, chunksize=1000)
            
            for i, result in enumerate(results):
                transformed_lines.append(result)
                
                # Print progress every 100,000 rows
                if (i + 1) % 100000 == 0 or (i + 1) == total_rows:
                    percentage = ((i + 1) / total_rows) * 100
                    elapsed = time.time() - start_transform
                    rows_per_second = (i + 1) / elapsed if elapsed > 0 else 0
                    sys.stdout.write(f"\rTransforming data: {percentage:.2f}% | {i+1:,.0f}/{total_rows:,.0f} rows | {rows_per_second:,.2f} rows/s")
                    sys.stdout.flush()
        
        sys.stdout.write("\n") # Newline after the progress bar is complete
        elapsed_transform = time.time() - start_transform
        print(f"Data transformation finished in {elapsed_transform:.2f} seconds.")

        # Filter out any lines that failed to parse
        cleaned_lines = [line for line in transformed_lines if line is not None]
        cleaned_count = len(cleaned_lines)
        skipped_count = total_rows - cleaned_count
        
        # Step 4: Stream the cleaned data to the database using COPY
        print(f"Starting COPY to ingest {cleaned_count:,} rows into the database...")
        
        # Create an in-memory file-like object from the transformed data
        data_to_copy = io.StringIO("".join(cleaned_lines))
        
        with conn.cursor() as cur:
            start_copy = time.time()
            sql = f"COPY {table_name} (timestamp, frequency) FROM STDIN WITH (FORMAT csv, DELIMITER ',')"
            cur.copy_expert(sql, data_to_copy)
            conn.commit()
            elapsed_copy = time.time() - start_copy
            
        print(f"COPY finished in {elapsed_copy:.2f} seconds.")
        print(f"Ingestion rate: {cleaned_count / elapsed_copy:,.2f} rows/s")
        
        print(f"\nTotal rows ingested: {cleaned_count:,}")
        if skipped_count > 0:
            print(f"Total rows skipped due to parsing errors: {skipped_count:,}")
            
    except Exception as e:
        print(f"\nAn unexpected error occurred during ingestion: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    # --- Three different use cases ---
    ingest_parallel(Path(__file__).parent.parent / "data" / "Sollfrequenz.csv", "swissgrid_frequency_data")  # Original dataset
    #ingest_parallel(Path(__file__).parent.parent / "data" / "sollfrequenz_31M.csv", "volume_frequency_data")  # Persistent dataset
    #ingest_parallel(Path(__file__).parent.parent / "data" / "sollfrequenz_31M.csv", "stresstest_frequency_data")  # Live Stress Test
    # ---------------------------------
    