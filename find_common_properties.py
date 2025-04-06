import sqlite3
import pandas as pd

def find_common_properties(db_name='altos_one.db', output_file='common_properties.csv'):
    conn = sqlite3.connect(db_name)
    
    query = """
    SELECT 
        l.date,
        l.property_id AS listings_property_id,
        l.listing_id,
        p.pending_id,
        l.county_fips_code AS county_fips,
        l.street_address,
        l.city,
        l.state,
        l.zip,
        l.price AS listing_price,
        p.price AS pending_price,
        p.days_in_contract
    FROM listings l
    INNER JOIN pendings p 
        ON l.date = p.date
       AND l.listing_id = p.pending_id
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    total_common = len(df)
    # Ensure days_in_contract is numeric (if not, convert it)
    df['days_in_contract'] = pd.to_numeric(df['days_in_contract'], errors='coerce')
    common_with_days = df[df['days_in_contract'] > 0]
    total_with_days = len(common_with_days)
    
    print(f"Total common rows identified: {total_common}")
    print(f"Total common rows with days_in_contract > 0: {total_with_days}")
    
    df.to_csv(output_file, index=False)
    print(f"Common properties exported to {output_file}")

if __name__ == "__main__":
    find_common_properties()
