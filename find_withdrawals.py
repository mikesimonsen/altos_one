import sqlite3
import pandas as pd
from datetime import datetime, timedelta


def find_withdrawn_listings(conn, target_week_str, filter_clause=""):
    target_week = datetime.strptime(target_week_str, '%Y-%m-%d')
    week_prior_str = (target_week - timedelta(days=7)).strftime('%Y-%m-%d')

    query = f"""
    WITH target_props AS (
        SELECT property_id FROM listings WHERE date = ? {filter_clause}
        UNION
        SELECT property_id FROM pendings WHERE date = ? {filter_clause}
    )
    SELECT l.*
    FROM listings l
    WHERE l.date = ? {filter_clause}
      AND l.property_id NOT IN (SELECT property_id FROM target_props)
    """
    df = pd.read_sql_query(query, conn, params=(target_week_str, target_week_str, week_prior_str))
    return df, week_prior_str


def compute_state_counts(conn, week_prior_str, filter_clause=""):
    query = f"""
    SELECT state, COUNT(*) AS prior_week_listings
    FROM listings
    WHERE date = ? {filter_clause}
    GROUP BY state
    """
    return pd.read_sql_query(query, conn, params=(week_prior_str,))


def compute_metro_counts(conn, week_prior_str, filter_clause=""):
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
    withdrawn_counts = withdrawals_df.groupby(group_col).size().reset_index(name='withdrawn_count')
    stats = pd.merge(counts_df, withdrawn_counts, on=group_col, how='left')
    stats['withdrawn_count'] = stats['withdrawn_count'].fillna(0)
    stats['withdrawal_percentage'] = (
        stats['withdrawn_count'] / stats['prior_week_listings'].replace({0: pd.NA}) * 100
    ).round(2).fillna(0)
    return stats


def add_metro_column(df, conn):
    mapping = pd.read_sql_query("SELECT zipcode, metro, display_name FROM zip_to_metro", conn)
    merged = pd.merge(df, mapping, left_on='zip', right_on='zipcode', how='left')
    merged.drop(columns=['zipcode'], inplace=True)
    merged['metro'] = merged['metro'].fillna('UNKNOWN')
    return merged


def run_all_history(conn, filter_clause=""):
    dates = pd.read_sql_query("SELECT DISTINCT date FROM listings ORDER BY date", conn)['date']
    all_stats = []
    for d in dates:
        withdrawn_df, prior = find_withdrawn_listings(conn, d, filter_clause)
        state_counts = compute_state_counts(conn, prior, filter_clause)
        stats = compute_withdrawal_statistics(withdrawn_df, state_counts, 'state')
        stats.insert(0, 'date', d)
        all_stats.append(stats)
    result = pd.concat(all_stats, ignore_index=True)
    result.to_csv('withdrawn_history_stats.csv', index=False)
    print(f"✅ Exported historical state-level stats for {len(dates)} weeks to 'withdrawn_history_stats.csv'")


def run_detailed(conn, filter_clause=""):
    target_week = input("Enter the target week date (YYYY-MM-DD): ").strip()
    withdrawn_df, prior = find_withdrawn_listings(conn, target_week, filter_clause)

    withdrawn_df = add_metro_column(withdrawn_df, conn)

    market = input("Enter state code, metro filter, 'top50', or press Enter for all: ").strip()
    is_top50 = market.lower() == 'top50'

    if is_top50:
        top50 = pd.read_sql_query(
            "SELECT DISTINCT metro FROM zip_to_metro WHERE display_name IS NOT NULL", conn
        )['metro'].tolist()
        withdrawn_df = withdrawn_df[withdrawn_df['metro'].isin(top50)]
    elif len(market) == 2:
        withdrawn_df = withdrawn_df[withdrawn_df['state'].str.upper() == market.upper()]
    elif market:
        withdrawn_df = withdrawn_df[withdrawn_df['metro'].str.contains(market, case=False, na=False)]

    fn1 = input(f"Filename for withdrawn listings (default withdrawn_listings_{target_week}.csv): ").strip() or f"withdrawn_listings_{target_week}.csv"
    withdrawn_df.to_csv(fn1, index=False)
    print(f"✅ Exported withdrawn listings to '{fn1}'")

    state_counts = compute_state_counts(conn, prior, filter_clause)
    if is_top50:
        valid_states = withdrawn_df['state'].unique().tolist()
        state_counts = state_counts[state_counts['state'].isin(valid_states)]
    elif len(market) == 2:
        state_counts = state_counts[state_counts['state'].str.upper() == market.upper()]
    state_stats = compute_withdrawal_statistics(withdrawn_df, state_counts, 'state')
    fn2 = input(f"Filename for state stats (default withdrawn_stats_{target_week}.csv): ").strip() or f"withdrawn_stats_{target_week}.csv"
    state_stats.to_csv(fn2, index=False)
    print(f"✅ Exported state-level stats to '{fn2}'")

    metro_counts = compute_metro_counts(conn, prior, filter_clause)
    if is_top50:
        metro_counts = metro_counts[metro_counts['metro'].isin(top50)]
    elif market and len(market) != 2:
        metro_counts = metro_counts[metro_counts['metro'].str.contains(market, case=False, na=False)]
    metro_stats = compute_withdrawal_statistics(withdrawn_df, metro_counts, 'metro')

    # If top50, merge display_name
    if is_top50:
        display_map = pd.read_sql_query(
            "SELECT DISTINCT metro, display_name FROM zip_to_metro WHERE display_name IS NOT NULL", conn
        )
        metro_stats = metro_stats.merge(display_map, on='metro', how='left')
        # Reorder columns to include display_name after metro
        cols = metro_stats.columns.tolist()
        cols.insert(cols.index('metro')+1, cols.pop(cols.index('display_name')))
        metro_stats = metro_stats[cols]

    fn3 = input(f"Filename for metro stats (default withdrawn_metro_stats_{target_week}.csv): ").strip() or f"withdrawn_metro_stats_{target_week}.csv"
    metro_stats.to_csv(fn3, index=False)
    print(f"✅ Exported metro-level stats to '{fn3}'")


def main():
    conn = sqlite3.connect('altos_one.db')
    mode = input("Mode: all-history (enter 'all') or detailed single-week (enter 'detailed', default): ").strip().lower()
    sf = input("Focus only on single_family residences? (y/n, default y): ").strip().lower()
    filter_clause = '' if sf == 'n' else "AND type = 'single_family'"

    if mode == 'all':
        run_all_history(conn, filter_clause)
    else:
        run_detailed(conn, filter_clause)

    conn.close()

if __name__ == '__main__':
    main()
