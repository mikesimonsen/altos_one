import sqlite3
import pandas as pd

def main():
    # Connect to the SQLite database
    conn = sqlite3.connect('altos_one.db')

    # Prompt the user for a partial street address to search for
    address_part = input("Enter part of the street address to search for: ").strip()
    pattern = f"%{address_part}%"

    # Query each table for matching addresses, including a source column
    listings_query = (
        "SELECT 'listings' AS source, * "
        "FROM listings "
        "WHERE street_address LIKE ?"
    )
    pendings_query = (
        "SELECT 'pendings' AS source, * "
        "FROM pendings "
        "WHERE street_address LIKE ?"
    )
    solds_query = (
        "SELECT 'solds' AS source, * "
        "FROM solds "
        "WHERE street_address LIKE ?"
    )

    # Load into DataFrames
    df_listings = pd.read_sql_query(listings_query, conn, params=(pattern,))
    df_pendings = pd.read_sql_query(pendings_query, conn, params=(pattern,))
    df_solds = pd.read_sql_query(solds_query, conn, params=(pattern,))

    # Combine all results
    df_all = pd.concat([df_listings, df_pendings, df_solds], ignore_index=True, sort=False)

    # Close the connection
    conn.close()

    # Export to CSV
    safe_addr = address_part.replace(' ', '_')
    output_file = f"address_search_{safe_addr}.csv"
    df_all.to_csv(output_file, index=False)
    print(f"✅ Exported {len(df_all)} matching rows to '{output_file}'")

if __name__ == '__main__':
    main()
