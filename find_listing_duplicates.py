import sqlite3
import pandas as pd

def find_duplicates_from_db(db_name='altos_one.db', output_file='listings_duplicate.csv'):
    # Connect to the SQLite database and load the listings table into a DataFrame
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT * FROM listings", conn)
    conn.close()
    
    # Ensure the key columns are treated as strings and strip any extra whitespace
    df['date'] = df['date'].astype(str).str.strip()
    df['property_id'] = df['property_id'].astype(str).str.strip()
    df['listing_id'] = df['listing_id'].astype(str).str.strip()
    
    # Group by the key: date, property_id, listing_id, and filter to include only groups with duplicates
    duplicates = df.groupby(['date', 'property_id', 'listing_id']).filter(lambda group: len(group) > 1).copy()
    
    # Within each duplicate group, assign a sequential counter (starting at 1)
    duplicates['dupe'] = duplicates.groupby(['date', 'property_id', 'listing_id']).cumcount() + 1
    
    # Export the DataFrame (all columns plus the dupe column) to a CSV file
    duplicates.to_csv(output_file, index=False)
    print(f"Duplicate rows exported to {output_file}")

def main():
    output_file = input("Enter output CSV filename (default 'listings_duplicate.csv'): ") or "listings_duplicate.csv"
    find_duplicates_from_db(output_file=output_file)

if __name__ == "__main__":
    main()
