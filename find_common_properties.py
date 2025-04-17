import sqlite3
import pandas as pd

def main():
    conn = sqlite3.connect('altos_one.db')

    # 1) Prompt filters
    sf = input("Limit to single_family type? (y/n, default n): ").strip().lower()
    filter_clause = "AND type = 'single_family'" if sf == 'y' else ""

    date = input("Enter the target week date (YYYY-MM-DD): ").strip()
    check_solds = (
        input("Also restrict to properties present in solds? (y/n, default n): ")
        .strip().lower() == 'y'
    )

    # 2) Load listings & pendings (for that date)
    listings_q = f"""
        SELECT date, property_id, listing_id,
               county_fips_code, street_address,
               city, state, zip, price AS listing_price
        FROM listings
        WHERE date = ? {filter_clause}
    """
    pendings_q = f"""
        SELECT date, property_id, pending_id,
               county_fips_code, street_address,
               city, state, zip, price AS pending_price,
               days_in_contract
        FROM pendings
        WHERE date = ? {filter_clause}
    """
    df_list = pd.read_sql_query(listings_q, conn, params=(date,))
    df_pen  = pd.read_sql_query(pendings_q, conn, params=(date,))

    # 3) Union them (so any property in either table)
    df_list['in_list'] = True
    df_pen ['in_pen']  = True
    df_union = pd.merge(
        df_list, df_pen,
        on=['date','property_id','county_fips_code','street_address','city','state','zip'],
        how='outer',
        suffixes=('_list','_pen')
    )

    # 4) Optionally restrict to those in solds
    if check_solds:
        sold_ids = pd.read_sql_query("SELECT DISTINCT property_id FROM solds", conn)
        df_union = df_union[df_union['property_id'].isin(sold_ids['property_id'])]

    # 5) Bring in sold_date & sold_price
    df_solds = pd.read_sql_query(
        "SELECT property_id, sold_date, sold_price FROM solds", conn
    )
    df_union = df_union.merge(df_solds, on='property_id', how='left')

    conn.close()

    # 6) Clean up columns & pick one set of address fields
    #    We already have county_fips_code / street_address / city / state / zip from the merge
    #    listing_price, pending_price, days_in_contract may each be NaN if missing
    output_cols = [
        'date', 'property_id',
        'listing_id', 'pending_id',
        'county_fips_code', 'street_address',
        'city', 'state', 'zip',
        'listing_price', 'pending_price',
        'days_in_contract',
        'sold_date', 'sold_price'
    ]
    result = df_union[output_cols]

    # 7) Export
    default_fn = f"common_properties_{date}.csv"
    out_fn = input(f"Enter output CSV filename (default {default_fn}): ").strip() or default_fn
    result.to_csv(out_fn, index=False)
    print(f"✅ Exported {len(result)} rows to '{out_fn}'")

if __name__ == '__main__':
    main()
