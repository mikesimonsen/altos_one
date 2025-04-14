import sqlite3
import pandas as pd
from datetime import datetime

def insert_chunk(chunk, table_name, conn):
    """Attempt to insert a chunk via bulk insert.
       If it fails due to a unique constraint error, process each row individually 
       and print debugging info for rows that cause errors.
    """
    try:
        # Try bulk insertion with method "multi" for efficiency.
        chunk.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        return len(chunk)
    except sqlite3.IntegrityError as bulk_err:
        print("Bulk insert failed due to a UNIQUE constraint error. Switching to row-by-row insertion.")
        inserted = 0
        for idx, row in chunk.iterrows():
            try:
                # Convert the row (a Series) to a one-row DataFrame and insert it.
                pd.DataFrame([row]).to_sql(table_name, conn, if_exists='append', index=False)
                inserted += 1
            except sqlite3.IntegrityError as row_err:
                print(f"Unique constraint error for row index {idx}:")
                print(row)
                print("Error:", row_err)
        return inserted

def insert_csv_to_table(csv_file, table_name, db_name='altos_one.db', chunksize=10000):
    """Read the CSV file in chunks and insert each chunk into the specified table.
       If a UNIQUE constraint error is encountered in a chunk, each row is attempted
       individually, with error details printed for rows that cannot be inserted.
    """
    conn = sqlite3.connect(db_name)
    today = datetime.today().strftime('%Y-%m-%d')
    total_inserted = 0

    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        # Add the load_date column to the chunk.
        chunk['load_date'] = today
        # Optionally, you might want to ensure the date column is stripped, etc.
        if 'date' in chunk.columns:
            chunk['date'] = chunk['date'].astype(str).str.strip()
        inserted = insert_chunk(chunk, table_name, conn)
        total_inserted += inserted
        print(f"Inserted {inserted} rows into '{table_name}' from current chunk.")

    conn.close()
    print(f"Finished inserting into '{table_name}'. Total rows inserted: {total_inserted}")

def main():
    listings_csv = input("Enter the listings CSV filename (or press Enter to skip): ").strip()
    pendings_csv = input("Enter the pendings CSV filename (or press Enter to skip): ").strip()
    
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

if __name__ == "__main__":
    main()
