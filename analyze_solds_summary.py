import sqlite3
import pandas as pd

## this script is for calculating the list-to-sale ratio and aggregating solds data
def load_solds(db_path='altos_one.db'):
    """Load the solds table into a DataFrame."""
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT * FROM solds", conn)
    conn.close()
    return df


def process_solds(df):
    """Convert dates and numeric columns, derive sold_month."""
    df['sold_date'] = pd.to_datetime(df['sold_date'], errors='coerce')
    df['sold_month'] = df['sold_date'].dt.strftime('%Y-%m')
    df['sold_price'] = pd.to_numeric(df['sold_price'], errors='coerce')
    df['list_price_final'] = pd.to_numeric(df['list_price_final'], errors='coerce')
    return df


def load_zip_to_metro(db_path='altos_one.db'):
    """Load the zip_to_metro mapping (zipcode, metro, display_name)."""
    conn = sqlite3.connect(db_path)
    mapping = pd.read_sql_query("SELECT zipcode, metro, display_name FROM zip_to_metro", conn)
    conn.close()
    return mapping


def join_metro(df, mapping):
    """Merge solds DataFrame with mapping to add metro and display_name."""
    merged = pd.merge(df, mapping, left_on='zip', right_on='zipcode', how='left')
    merged.drop(columns=['zipcode'], inplace=True)
    merged['display_name'] = merged['display_name'].fillna('')
    merged['metro'] = merged['metro'].fillna('UNKNOWN')
    return merged


def calculate_ratio(df):
    """Compute sale_to_list_ratio and filter extremes."""
    def ratio_calc(row):
        sp = row['sold_price']
        lpf = row['list_price_final']
        if pd.notnull(sp) and pd.notnull(lpf) and lpf != 0:
            r = 1 + ((sp - lpf) / lpf)
            return r if 0.5 <= r <= 2.0 else None
        return None
    df['sale_to_list_ratio'] = df.apply(ratio_calc, axis=1)
    return df


def export_weekly_counts(df, output_file='sold_weeks_count.csv'):
    """Export CSV of sold counts by sold_month."""
    weeks_count = df.groupby('sold_month').size().reset_index(name='sold_count')
    weeks_count.to_csv(output_file, index=False)
    print(f"✅ Exported weekly sold counts to '{output_file}'")


def aggregate_summary(df, calc_ratio=False):
    """Aggregate summary by sold_month, market_name, type."""
    df['market_name'] = df.apply(
        lambda r: r['display_name'] if r['display_name'] else r['metro'], axis=1
    )
    agg_dict = {'sold_price': ['median', 'size']}
    if calc_ratio:
        agg_dict['sale_to_list_ratio'] = ['mean']
    summary = df.groupby(['sold_month', 'market_name', 'type']).agg(agg_dict)
    summary.columns = ['_'.join(col).strip() for col in summary.columns.values]
    summary = summary.reset_index()
    summary.rename(columns={'sold_price_median': 'median_sold_price',
                            'sold_price_size': 'sold_count'}, inplace=True)
    if calc_ratio:
        summary.rename(columns={'sale_to_list_ratio_mean': 'average_sale_to_list_ratio'}, inplace=True)
    return summary


def main():
    db_path = 'altos_one.db'
    # User options
    top50_choice = input("Include only top50 metros? (y/n, default n): ").strip().lower()
    filter_top50 = (top50_choice == 'y')
    calc_choice = input("Calculate list-to-sale ratio? (y/n, default n): ").strip().lower()
    calc_ratio = (calc_choice == 'y')

    # Load and process data
    df = load_solds(db_path)
    df = process_solds(df)
    mapping = load_zip_to_metro(db_path)
    df = join_metro(df, mapping)

    # Apply top50 filter
    if filter_top50:
        df = df[df['display_name'] != '']

    # Export weekly counts
    default_weeks = 'sold_weeks_count.csv'
    weeks_file = input(f"Enter filename for sold weeks count (default {default_weeks}): ").strip() or default_weeks
    export_weekly_counts(df, weeks_file)

    # Calculate ratio if requested
    if calc_ratio:
        df = calculate_ratio(df)

    # Aggregate and export summary
    summary = aggregate_summary(df, calc_ratio)
    default_summary = 'solds_summary_by_date.csv'
    summary_file = input(f"Enter filename for summary (default {default_summary}): ").strip() or default_summary
    summary.to_csv(summary_file, index=False)
    print(f"✅ Exported summary statistics to '{summary_file}' with {len(summary)} rows")

if __name__ == '__main__':
    main()
