import sqlite3
import pandas as pd
# This script creates a zip_to_metro table in the SQLite database and imports data from a CSV file.
# just a one time use
def create_zip_to_metro_table(db_name='altos_one.db'):
    """
    Creates (or re-creates) the zip_to_metro table with the columns:
      - metro  (from market_area)
      - zipcode
    An index on zipcode is added to optimize look-ups.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    # Drop the table if it exists; remove this step if you want to keep existing data.
    cursor.execute("DROP TABLE IF EXISTS zip_to_metro")
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS zip_to_metro (
        metro TEXT NOT NULL,
        zipcode TEXT NOT NULL
    );
    """
    cursor.execute(create_table_sql)
    
    # Create an index on the zipcode column for fast look-up.
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_zip ON zip_to_metro (zipcode);")
    
    conn.commit()
    conn.close()
    print("✅ Table 'zip_to_metro' created with an index on zipcode.")

def import_zip_to_metro(csv_file, db_name='altos_one.db'):
    """
    Reads the CSV file (expected columns: market_area, zipcode), renames the
    market_area column to 'metro', and imports the data into the zip_to_metro table.
    """
    # Read CSV file into a DataFrame.
    df = pd.read_csv(csv_file)
    
    # Rename the column 'market_area' to 'metro' if it exists.
    if 'market_area' in df.columns:
        df.rename(columns={'market_area': 'metro'}, inplace=True)
    else:
        print("Warning: 'market_area' column not found in the CSV. No renaming done.")
    
    # Optional: You can also trim whitespace from the columns.
    df['metro'] = df['metro'].astype(str).str.strip()
    df['zipcode'] = df['zipcode'].astype(str).str.strip()
    
    # Insert the data into the zip_to_metro table.
    conn = sqlite3.connect(db_name)
    df.to_sql('zip_to_metro', conn, if_exists='append', index=False)
    conn.commit()
    conn.close()
    print("✅ CSV data imported into 'zip_to_metro' table.")

def main():
    csv_file = input("Enter the zip-to-metro CSV filename: ").strip()
    if not csv_file:
        print("No CSV filename provided. Exiting.")
        return
    create_zip_to_metro_table()  # Create (or recreate) the table.
    import_zip_to_metro(csv_file) # Import the CSV data.
    print("Zip-to-metro mapping imported successfully.")

if __name__ == "__main__":
    main()
