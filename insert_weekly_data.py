import sqlite3
import pandas as pd
from datetime import datetime

def insert_csv_to_table(csv_file, table_name, db_name='altos_one.db', chunksize=10000):
    conn = sqlite3.connect(db_name)
    today = datetime.today().strftime('%Y-%m-%d')
    inserted_total = 0
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        # Add the load_date column (each table has a load_date column)
        chunk['load_date'] = today
        # Insert the entire chunk as-is.
        chunk.to_sql(table_name, conn, if_exists='append', index=False)
        inserted_total += len(chunk)
        print(f"Inserted {len(chunk)} rows into '{table_name}' from current chunk.")
    conn.close()
    print(f"Finished inserting into '{table_name}'. Total rows inserted: {inserted_total}")

def main():
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
