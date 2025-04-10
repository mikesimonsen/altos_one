import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def find_withdrawn_properties(conn, target_week_str, filter_clause=""):
    """
    Given a connection and a target week date (YYYY-MM-DD),
    compute the week-prior date, then return a DataFrame of withdrawn listings.
    A withdrawn property is one that was listed in the week prior and does not appear
    (by property_id) in either listings or pendings for the target week.
    Returns a tuple: (DataFrame, week_prior_str)
    """
    # Compute week-prior date by subtracting 7 days from target week.
    target_week = datetime.strptime(target_week_str, '%Y-%m-%d')
    week_prior = target_week - timedelta(days=7)
    week_prior_str = week_prior.strftime('%Y-%m-%d')
    
    query = f"""
    WITH target_props AS (
        SELECT property_id FROM listings WHERE date = ? {filter_clause}
        UNION
        SELECT property_id FROM pendings WHERE date = ? {filter_clause}
    )
    SELECT 
        l.date AS listed_date,
        l.street_address,
        l.city,
        l.state,
        l.zip,
        l.price,
        l.listing_id,
        l.property_id
    FROM listings l
    WHERE l.date = ? {filter_clause}
      AND l.property_id NOT IN (SELECT property_id FROM target_props)
    """
    
    params = (target_week_str, target_week_str, week_prior_str)
    df = pd.read_sql_query(query, conn, params=params)
    return df, week_prior_str

def compute_state_withdrawals(conn, week_prior_str, filter_clause=""):
    """
    Compute the total listings per state on the week-prior date.
    Returns a DataFrame with columns: state, prior_week_listings.
    """
    query = f"""
    SELECT state, COUNT(*) AS prior_week_listings
    FROM listings
    WHERE date = ? {filter_clause}
    GROUP BY state
    """
    df = pd.read_sql_query(query, conn, params=(week_prior_str,))
    return df

def compute_withdrawal_stats(withdrawals_df, listings_counts_df):
    """
    Merge withdrawn property counts per state with total listings counts,
    and calculate the percentage withdrawn per state.
    Returns a DataFrame with columns: state, withdrawn_count, prior_week_listings, withdrawal_percentage.
    """
    # Count withdrawn properties per state from the withdrawals DataFrame.
    state_withdrawn = withdrawals_df.groupby('state').size().reset_index(name='withdrawn_count')
    
    # Merge with total prior week listings per state.
    stats = pd.merge(listings_counts_df, state_withdrawn, on='state', how='left')
    stats['withdrawn_count'] = stats['withdrawn_count'].fillna(0)
    
    # Calculate withdrawal percentage.
    stats['withdrawal_percentage'] = stats.apply(
        lambda row: round((row['withdrawn_count'] / row['prior_week_listings']) * 100, 2)
                    if row['prior_week_listings'] > 0 else 0, axis=1)
    return stats

def main():
    # Prompt for a single target week date.
    target_week_str = input("Enter the target week date (YYYY-MM-DD): ").strip()
    
    # Ask if we should focus on single_family residences.
    focus_sf = input("Focus only on single_family residences? (y/n): ").strip().lower()
    if focus_sf == 'y':
        filter_clause = "AND type = 'single_family'"
    else:
        filter_clause = ""
    
    # Connect to the database.
    conn = sqlite3.connect('altos_one.db')
    
    # Find withdrawn properties: from listings on week-prior that do NOT appear in target week.
    withdrawals_df, week_prior_str = find_withdrawn_properties(conn, target_week_str, filter_clause)
    print(f"Found {len(withdrawals_df)} withdrawn properties (listed on {week_prior_str} and absent on {target_week_str}).")
    
    # Build default output filenames incorporating the target date.
    default_withdrawn_filename = f"withdrawn_listings_{target_week_str}.csv"
    withdrawn_out = input(f"Enter output filename for withdrawn properties (default: {default_withdrawn_filename}): ").strip() or default_withdrawn_filename
    withdrawals_df.to_csv(withdrawn_out, index=False)
    print(f"Withdrawn properties exported to {withdrawn_out}")
    
    # Compute total listings per state for the week-prior.
    listings_counts_df = compute_state_withdrawals(conn, week_prior_str, filter_clause)
    
    # Compute state-level withdrawal stats.
    stats_df = compute_withdrawal_stats(withdrawals_df, listings_counts_df)
    default_stats_filename = f"withdrawn_stats_{target_week_str}.csv"
    stats_out = input(f"Enter output filename for state-level withdrawal stats (default: {default_stats_filename}): ").strip() or default_stats_filename
    stats_df.to_csv(stats_out, index=False)
    print(f"Withdrawal stats exported to {stats_out}")
    
    conn.close()

if __name__ == "__main__":
    main()
