import csv
from datetime import datetime, timedelta
import locale
from pathlib import Path

def generate_full_year_csv(start_date, end_date, output_file):
    """
    Generates a CSV file with two columns: 'Datum Zeit' and 'A:f_soll_aktiv [Hz]'.
    The data covers a full year with a 1-second resolution, with a constant
    frequency value of 50 Hz. This is a memory-efficient solution that writes
    row by row to the file.

    Args:
        start_date (datetime): The starting date and time for the dataset.
        end_date (datetime): The end date and time for the dataset.
        output_file (Path): The Path object for the output CSV file.
    """
    # Set the locale to German for correct weekday abbreviation (e.g., Sa.)
    try:
        locale.setlocale(locale.LC_TIME, 'de_DE.UTF-8')
    except locale.Error:
        try:
            locale.setlocale(locale.LC_TIME, 'de_DE.utf8')
        except locale.Error:
            try:
                locale.setlocale(locale.LC_TIME, 'German')
            except locale.Error:
                print("Warning: Could not set German locale. Weekday abbreviations may not be correct.")

    total_seconds = int((end_date - start_date).total_seconds())
    current_time = start_date
    progress_step = total_seconds // 10  # Update progress every 10%
    
    print(f"Starting CSV generation for a total of {total_seconds} seconds...")

    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=';')
        
        # Write the header row
        writer.writerow(['Datum Zeit', 'A:f_soll_aktiv [Hz]'])
        
        for i in range(total_seconds + 1):
            if i > 0 and i % progress_step == 0:
                print(f"Progress: {i / total_seconds * 100:.0f}% ({i}/{total_seconds} rows)")

            # Format the timestamp string to match the original CSV format
            formatted_date = current_time.strftime('%a. %d.%m.%y %H:%M:%S')
            
            # Write the data row
            writer.writerow([formatted_date, 50])
            
            # Increment by one second
            current_time += timedelta(seconds=1)
            
    print(f"CSV generation complete. File saved to '{output_file}'")

if __name__ == "__main__":
    # Define the start and end dates for a full year
    start_date = datetime(2021, 5, 1, 0, 0, 0)
    end_date = datetime(2022, 4, 30, 23, 59, 59) # A full year from the start date

    output_dir = Path(__file__).parent.parent / "data"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / "sollfrequenz_31M.csv"

    generate_full_year_csv(start_date, end_date, output_path)
