import sqlite3
import pandas as pd
from datetime import datetime

def load_solds(db_name='altos_one.db'):
    """Load the solds table from the database into a DataFrame."""
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT * FROM solds", conn)
    conn.close()
    return df

def load_zip_to_metro(db_name='altos_one.db'):
    """Load the zip_to_metro mapping table into a DataFrame."""
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT zipcode, metro FROM zip_to_metro", conn)
    conn.close()
    return df

def process_solds(df):
    """
    Convert sold_date to datetime and derive sold_month (YYYY-MM).
    Convert sold_price and list_price_final to numeric.
    """
    # Convert sold_date to datetime (invalid dates become NaT)
    df['sold_date'] = pd.to_datetime(df['sold_date'], errors='coerce')
    # Create sold_month in YYYY-MM format based on sold_date
    df['sold_month'] = df['sold_date'].dt.strftime('%Y-%m')
    # Convert sold_price and list_price_final to numeric (invalid to NaN)
    df['sold_price'] = pd.to_numeric(df['sold_price'], errors='coerce')
    df['list_price_final'] = pd.to_numeric(df['list_price_final'], errors='coerce')
    return df

def join_metro(solds_df, zip_df):
    """
    Merge the solds DataFrame with the zip_to_metro mapping table on zip = zipcode.
    Missing metro values are set to 'UNKNOWN'.
    """
    merged = pd.merge(solds_df, zip_df, left_on='zip', right_on='zipcode', how='left')
    merged.drop(columns=['zipcode'], inplace=True)
    merged['metro'] = merged['metro'].fillna('UNKNOWN')
    return merged

def compute_aggregates(df, calc_ratio=False):
    """
    Group the solds DataFrame by sold_month, metro, and type.
    Always compute:
      - sold_count: the number of sold records.
      - median_sold_price: the median sold_price.
    If calc_ratio is True, compute for each group:
      - average_sale_to_list_ratio: the average of the row-level ratio, computed as 
          1 + ((sold_price - list_price_final)/list_price_final)
      Rows where either sold_price or list_price_final is null or list_price_final is zero
      result in a null ratio. Moreover, if the computed ratio is greater than 2.0 (200%) 
      or less than 0.5 (50%), the ratio is ignored (set to null) for aggregation.
    """
    if calc_ratio:
        # Compute the ratio for each row.
        df['list_to_sale_ratio'] = df.apply(
            lambda row: 1 + ((row['sold_price'] - row['list_price_final']) / row['list_price_final'])
            if (pd.notnull(row['sold_price']) and pd.notnull(row['list_price_final']) and row['list_price_final'] != 0)
            else None, axis=1
        )
        # Exclude rows where the ratio is > 2.0 or < 0.5.
        df.loc[(df['list_to_sale_ratio'] > 2) | (df['list_to_sale_ratio'] < 0.5), 'list_to_sale_ratio'] = None

    # Define basic aggregations.
    agg_dict = {
        'sold_price': ['median', 'size']
    }
    if calc_ratio:
        agg_dict['list_to_sale_ratio'] = ['mean']
    
    grouped = df.groupby(['sold_month', 'metro', 'type']).agg(agg_dict)
    # Flatten multi-index columns.
    grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
    grouped = grouped.reset_index()

    # Rename columns: sold_price_median -> median_sold_price, sold_price_size -> sold_count.
    grouped.rename(columns={'sold_price_median': 'median_sold_price',
                            'sold_price_size': 'sold_count'}, inplace=True)
    # Rename the ratio column to average_sale_to_list_ratio if calculated.
    if calc_ratio:
        grouped.rename(columns={'list_to_sale_ratio_mean': 'average_sale_to_list_ratio'}, inplace=True)
    return grouped

def main():
    calc_choice = input("Calculate list-to-sale ratio? (y/n, default n): ").strip().lower()
    calc_ratio = (calc_choice == 'y')
    
    # Load solds data
    solds_df = load_solds()
    solds_df = process_solds(solds_df)
    
    # Load zip_to_metro mapping and join to add metro column.
    zip_df = load_zip_to_metro()
    solds_df = join_metro(solds_df, zip_df)
    
    # Drop rows that don't have a valid sold_month.
    solds_df = solds_df.dropna(subset=['sold_month'])
    
    summary_df = compute_aggregates(solds_df, calc_ratio=calc_ratio)
    
    default_out = "solds_summary.csv"
    out_file = input(f"Enter output CSV filename (default: {default_out}): ").strip() or default_out
    summary_df.to_csv(out_file, index=False)
    print(f"Summary statistics exported to {out_file}")

if __name__ == "__main__":
    main()
