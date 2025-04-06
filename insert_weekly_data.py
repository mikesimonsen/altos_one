import sqlite3
import pandas as pd
from datetime import datetime

def insert_csv_to_table(csv_file, table_name, db_name='altos_one.db', chunksize=10000):
    conn = sqlite3.connect(db_name)
    today = datetime.today().strftime('%Y-%m-%d')
    inserted_total = 0
    
    for chunk in pd.read_csv(csv_file, chunksize=chunksize):
        # Add load_date column to the chunk
        chunk['load_date'] = today
        
        # Insert the chunk exactly as-is
        # If a row violates the UNIQUE constraint, it will be ignored.
        chunk.to_sql(table_name, conn, if_exists='append', index=False, method='multi')
        inserted_total += len(chunk)
        print(f"Inserted {len(chunk)} rows into '{table_name}' from current chunk.")
    
    conn.close()
    print(f"Finished inserting into '{table_name}'. Total rows processed: {inserted_total}")

def main():
    listings_csv = input("Enter the listings CSV filename: ")
    pendings_csv = input("Enter the pendings CSV filename: ")
    
    insert_csv_to_table(listings_csv, 'listings')
    insert_csv_to_table(pendings_csv, 'pendings')

if __name__ == "__main__":
    main()
