import sqlite3
import pandas as pd

def find_address(address_part, db_name='altos_one.db'):
    conn = sqlite3.connect(db_name)
    
    # Parameterized query using LIKE with wildcards
    query_listings = """
        SELECT 'listings' AS source, *
        FROM listings
        WHERE street_address LIKE ?
    """
    query_pendings = """
        SELECT 'pendings' AS source, *
        FROM pendings
        WHERE street_address LIKE ?
    """
    
    param = f"%{address_part}%"
    df_listings = pd.read_sql_query(query_listings, conn, params=(param,))
    df_pendings = pd.read_sql_query(query_pendings, conn, params=(param,))
    conn.close()
    
    # Combine results from both tables
    df = pd.concat([df_listings, df_pendings], ignore_index=True)
    return df

def main():
    search_term = input("Enter a portion of a street address to search for: ").strip()
    if not search_term:
        print("No search term entered. Exiting.")
        return
    
    df = find_address(search_term)
    total_found = len(df)
    print(f"\nFound {total_found} rows matching '{search_term}':\n")
    print(df)
    
    export = input("\nExport results to CSV? (y/n): ").strip().lower()
    if export == 'y':
        out_file = input("Enter output filename (default: found_addresses.csv): ").strip() or "found_addresses.csv"
        df.to_csv(out_file, index=False)
        print(f"Results exported to {out_file}")

if __name__ == "__main__":
    main()
