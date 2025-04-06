import sqlite3

def initialize_database(db_name='altos_one.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Drop existing tables if they exist
    cursor.execute("DROP TABLE IF EXISTS listings")
    cursor.execute("DROP TABLE IF EXISTS pendings")
    
    # Create listings table with composite key (date, listing_id)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        date TEXT NOT NULL,
        property_id INTEGER,
        listing_id INTEGER NOT NULL,
        parcel_number TEXT,
        county_fips_code TEXT,
        street_address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        price INTEGER,
        type TEXT,
        beds INTEGER,
        baths REAL,
        floor_size INTEGER,
        lot_size INTEGER,
        built_in INTEGER,
        geo_lat REAL,
        geo_long REAL,
        load_date TEXT,
        UNIQUE(date, listing_id)
    )
    """)
    
    # Create pendings table with composite key (date, pending_id)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS pendings (
        date TEXT NOT NULL,
        property_id INTEGER,
        pending_id INTEGER NOT NULL,
        parcel_number TEXT,
        county_fips_code TEXT,
        street_address TEXT,
        city TEXT,
        state TEXT,
        zip TEXT,
        price INTEGER,
        type TEXT,
        beds INTEGER,
        baths REAL,
        floor_size INTEGER,
        lot_size INTEGER,
        built_in INTEGER,
        geo_lat REAL,
        geo_long REAL,
        days_on_market INTEGER,
        agent_name TEXT,
        agent_email TEXT,
        agent_phone TEXT,
        agent_office TEXT,
        days_in_contract INTEGER,
        load_date TEXT,
        UNIQUE(date, pending_id)
    )
    """)
    
    # Create indexes to speed up queries and joins
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_listings_date ON listings (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_listings_listing_id ON listings (listing_id)')
    
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pendings_date ON pendings (date)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_pendings_pending_id ON pendings (pending_id)')
    
    conn.commit()
    conn.close()
    print("✅ Database initialized with composite UNIQUE constraints on (date, listing_id) and (date, pending_id).")

if __name__ == "__main__":
    initialize_database()
