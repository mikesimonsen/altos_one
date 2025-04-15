import sqlite3

def create_solds_table(db_name='altos_one.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Drop the solds table if it exists
    cursor.execute("DROP TABLE IF EXISTS solds")
    
    # Create the solds table with the specified columns (including load_date)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS solds (
        date TEXT NOT NULL,
        property_id INTEGER,
        county_fips_code TEXT,
        parcel_number TEXT,
        street_address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        county TEXT,
        type TEXT,
        beds INTEGER,
        baths REAL,
        floor_size INTEGER,
        lot_size INTEGER,
        built_in INTEGER,
        geo_lat REAL,
        geo_long REAL,
        estimated_value REAL,
        sold_date TEXT,
        sold_price INTEGER,
        list_price_initial INTEGER,
        list_price_final INTEGER,
        listed_on TEXT,
        pending_on TEXT,
        agent_name TEXT,
        agent_email TEXT,
        agent_phone TEXT,
        agent_office TEXT,
        load_date TEXT
    )
    """)
    
    # Create indexes for faster lookup similar to listings and pendings
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solds_property_id ON solds(property_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solds_date ON solds(date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solds_zip ON solds(zip)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_solds_street_address ON solds(street_address)')
    
    conn.commit()
    conn.close()
    print("✅ 'solds' table created with load_date column and appropriate indexes.")

if __name__ == "__main__":
    create_solds_table()
