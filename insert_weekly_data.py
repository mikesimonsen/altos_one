import sqlite3
import pandas as pd
from datetime import datetime

def insert_csv_to_table(csv_file: str, table_name: str, db_name: str = 'altos_one.db', chunksize: int = 10000) -> None:
    """
    Insert CSV data into the specified SQLite table. If primary key conflicts occur,
    replaces existing rows. Adds a 'load_date' column to each row.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    today = datetime.today().strftime('%Y-%m-%d')
    inserted_total = 0

    # Read in chunks to handle large files
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        # Handle column name changes for solds table
        if table_name == 'solds':
            if 'listed_price' in chunk.columns:
                chunk = chunk.rename(columns={'listed_price': 'list_price_initial'})
            if 'pending_price' in chunk.columns:
                chunk = chunk.rename(columns={'pending_price': 'list_price_final'})

        # Add the load_date column (each table has a load_date column)
        chunk['load_date'] = today

        # Prepare for upsert: use INSERT OR REPLACE for SQLite
        cols = chunk.columns.tolist()
        placeholders = ",".join(["?" for _ in cols])
        col_names = ",".join(cols)
        insert_sql = f"INSERT OR REPLACE INTO {table_name} ({col_names}) VALUES ({placeholders})"

        # Convert DataFrame rows to list of tuples for executemany
        data_tuples = [tuple(x) for x in chunk[cols].values]

        try:
            cursor.executemany(insert_sql, data_tuples)
            conn.commit()
            inserted_total += len(data_tuples)
            print(f"Inserted/Replaced {len(data_tuples)} rows into '{table_name}' from current chunk.")
        except sqlite3.IntegrityError as e:
            print(f"IntegrityError encountered: {e}. Continuing with next chunk.")

    conn.close()
    print(f"Finished inserting into '{table_name}'. Total rows processed: {inserted_total}")


def main() -> None:
    """
    Prompt user for CSV filenames for listings, pendings, and solds,
    and insert into corresponding tables.
    """
    listings_csv = input("Enter the listings CSV filename (or press Enter to skip): ").strip()
    pendings_csv = input("Enter the pendings CSV filename (or press Enter to skip): ").strip()
    solds_csv = input("Enter the solds CSV filename (or press Enter to skip): ").strip()
    
    if listings_csv:
        print("Importing listings...")
        insert_csv_to_table(listings_csv, 'listings')
    else:
        print("Skipping listings import.")
    
    if pendings_csv:
        print("Importing pendings...")
        insert_csv_to_table(pendings_csv, 'pendings')
    else:
        print("Skipping pendings import.")
    
    if solds_csv:
        print("Importing solds...")
        insert_csv_to_table(solds_csv, 'solds')
    else:
        print("Skipping solds import.")

if __name__ == "__main__":
    main()