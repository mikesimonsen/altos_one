import sqlite3
import pandas as pd
from datetime import datetime, timedelta

def find_withdrawn_properties(conn, target_week_str, filter_clause=""):
    """
    Computes the week-prior date (target_week - 7 days), then returns a DataFrame
    of withdrawn properties from last week's listings (filtered by filter_clause) that do NOT
    appear (by property_id) in either listings or pendings on the target week.
    
    Returns a tuple: (withdrawals_df, week_prior_str)
    """
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
    # For this query, we use target_week_str for target week in the subquery,
    # and week_prior_str for selecting last week’s listings.
    params = (target_week_str, target_week_str, week_prior_str)
    df = pd.read_sql_query(query, conn, params=params)
    return df, week_prior_str

def add_metro_column(withdrawals_df, conn):
    """
    Merge the withdrawn properties DataFrame with the zip_to_metro table
    (which has columns: zipcode and metro) to add a metro column.
    """
    try:
        zip_to_metro = pd.read_sql_query("SELECT zipcode, metro FROM zip_to_metro", conn)
        merged_df = pd.merge(withdrawals_df, zip_to_metro, left_on='zip', right_on='zipcode', how='left')
        merged_df.drop(columns=['zipcode'], inplace=True)
        # Fill null metro values with 'UNKNOWN'
        merged_df['metro'] = merged_df['metro'].fillna('UNKNOWN')
        return merged_df
    except Exception as e:
        print("Warning: Could not merge with zip_to_metro:", e)
        withdrawals_df['metro'] = "UNKNOWN"
        return withdrawals_df

def compute_state_counts(conn, week_prior_str, filter_clause=""):
    """
    Query the listings table for the week-prior date and compute total listings count per state.
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

def compute_metro_counts(conn, week_prior_str, filter_clause=""):
    """
    Query the listings table for the week-prior date joined with the zip_to_metro table 
    to compute total listings count per metro. Returns a DataFrame with columns: metro, prior_week_listings.
    """
    query = f"""
    SELECT z.metro, COUNT(*) AS prior_week_listings
    FROM listings l
    LEFT JOIN zip_to_metro z ON l.zip = z.zipcode
    WHERE l.date = ? {filter_clause}
    GROUP BY z.metro
    """
    df = pd.read_sql_query(query, conn, params=(week_prior_str,))
    df['metro'] = df['metro'].fillna('UNKNOWN')
    return df

def compute_withdrawal_statistics(withdrawals_df, counts_df, group_col):
    """
    Merge the counts (grouped by group_col, e.g., state or metro) from the listings (for the week-prior)
    with the withdrawn counts (grouped by group_col) from the withdrawals DataFrame.
    Compute the withdrawal_percentage = (withdrawn_count / prior_week_listings) * 100.
    Returns a DataFrame with columns: group_col, withdrawn_count, prior_week_listings, withdrawal_percentage.
    """
    withdrawn_counts = withdrawals_df.groupby(group_col).size().reset_index(name='withdrawn_count')
    stats = pd.merge(counts_df, withdrawn_counts, on=group_col, how='left')
    stats['withdrawn_count'] = stats['withdrawn_count'].fillna(0)
    stats['withdrawal_percentage'] = stats.apply(
        lambda row: round((row['withdrawn_count'] / row['prior_week_listings']) * 100, 2)
                    if row['prior_week_listings'] > 0 else 0, axis=1)
    return stats

def main():
    # Prompt for the target week date (the date that represents the current week)
    target_week_str = input("Enter the target week date (YYYY-MM-DD): ").strip()
    
    # Prompt for filtering: either by state or metro (default: all)
    filter_val = input("Enter a specific state (2-letter code) or metro market to filter (press Enter for all): ").strip()
    
    # Setup the additional filter clause based on the input.
    # We'll apply this filter after loading the data (on the withdrawals DataFrame).
    filter_clause = ""  # for SQL query, no additional filter is applied.
    
    conn = sqlite3.connect('altos_one.db')
    
    # Retrieve withdrawn properties (from listings on week_prior not found in target week).
    withdrawals_df, week_prior_str = find_withdrawn_properties(conn, target_week_str, filter_clause)
    
    # Merge with zip_to_metro to add a 'metro' column.
    withdrawals_df = add_metro_column(withdrawals_df, conn)
    
    # If a filter value was provided, apply it.
    if filter_val:
        if len(filter_val) == 2:  # assume state code
            withdrawals_df = withdrawals_df[withdrawals_df['state'].str.upper() == filter_val.upper()]
        else:
            # Assume it's a metro market; use case-insensitive matching.
            withdrawals_df = withdrawals_df[withdrawals_df['metro'].str.contains(filter_val, case=False, na=False)]
    
    print(f"Found {len(withdrawals_df)} withdrawn properties (listed on {week_prior_str} and absent on {target_week_str}).")
    
    # Set default output filenames incorporating the target week date.
    default_withdrawn_filename = f"withdrawn_listings_{target_week_str}.csv"
    withdrawn_out = input(f"Enter output filename for withdrawn properties (default: {default_withdrawn_filename}): ").strip() or default_withdrawn_filename
    withdrawals_df.to_csv(withdrawn_out, index=False)
    print(f"Withdrawn properties exported to {withdrawn_out}")
    
    # State-level stats:
    state_counts_df = compute_state_counts(conn, week_prior_str, filter_clause)
    # If filtering by state, filter the counts DataFrame accordingly.
    if filter_val and len(filter_val) == 2:
        state_counts_df = state_counts_df[state_counts_df['state'].str.upper() == filter_val.upper()]
    
    state_stats = compute_withdrawal_statistics(withdrawals_df, state_counts_df, "state")
    default_state_filename = f"withdrawn_stats_{target_week_str}.csv"
    state_out = input(f"Enter output filename for state-level stats (default: {default_state_filename}): ").strip() or default_state_filename
    state_stats.to_csv(state_out, index=False)
    print(f"State-level withdrawal stats exported to {state_out}")
    
    # Metro-level stats:
    metro_counts_df = compute_metro_counts(conn, week_prior_str, filter_clause)
    # If filtering by metro, filter the counts DataFrame accordingly.
    if filter_val and len(filter_val) != 2:
        metro_counts_df = metro_counts_df[metro_counts_df['metro'].str.contains(filter_val, case=False, na=False)]
    
    metro_stats = compute_withdrawal_statistics(withdrawals_df, metro_counts_df, "metro")
    default_metro_filename = f"withdrawn_metro_stats_{target_week_str}.csv"
    metro_out = input(f"Enter output filename for metro-level stats (default: {default_metro_filename}): ").strip() or default_metro_filename
    metro_stats.to_csv(metro_out, index=False)
    print(f"Metro-level withdrawal stats exported to {metro_out}")
    
    conn.close()

if __name__ == "__main__":
    main()
