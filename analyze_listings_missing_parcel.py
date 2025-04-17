import sqlite3
import pandas as pd
import os

def analyze_missing_parcels(db_name='altos_one.db', output_file="listings_missing_parcel_by_week_and_metro.csv"):
    try:
        conn = sqlite3.connect(db_name)
    except Exception as e:
        print(f"❌ Failed to connect to database '{db_name}': {e}")
        return

    # 1) Load listings data
    try:
        listings = pd.read_sql_query("SELECT date, zip, parcel_number FROM listings", conn)
        print(f"Loaded {len(listings)} rows from listings.")
    except Exception as e:
        print(f"❌ Error querying listings table: {e}")
        conn.close()
        return

    # 2) Load zip_to_metro mapping
    try:
        zip_to_metro = pd.read_sql_query("SELECT zipcode, metro FROM zip_to_metro", conn)
        print(f"Loaded {len(zip_to_metro)} rows from zip_to_metro.")
    except Exception as e:
        print(f"❌ Error querying zip_to_metro table: {e}")
        conn.close()
        return
    finally:
        conn.close()

    # 3) Merge to get metro
    df = pd.merge(listings, zip_to_metro,
                  left_on='zip', right_on='zipcode',
                  how='left')
    df.drop(columns=['zipcode'], inplace=True)
    df['metro'] = df['metro'].fillna('UNKNOWN')

    # 4) Filter missing parcel_number
    missing = df[
        df['parcel_number'].isnull() |
        (df['parcel_number'].astype(str).str.strip() == "")
    ]
    print(f"Found {len(missing)} listings with missing parcel_number.")

    # 5) Group by date & metro
    result = (
        missing
        .groupby(['date', 'metro'])
        .size()
        .reset_index(name='missing_parcel_count')
    )
    print(f"Result has {len(result)} rows (date × metro combinations).")

    # 6) Write CSV
    try:
        result.to_csv(output_file, index=False)
    except Exception as e:
        print(f"❌ Failed to write CSV '{output_file}': {e}")
        return

    # 7) Verify file
    if os.path.exists(output_file):
        print(f"✅ Exported missing-parcel summary to '{output_file}'")
    else:
        print(f"❌ Expected output file '{output_file}' not found!")

def main():
    analyze_missing_parcels()

if __name__ == "__main__":
    main()
