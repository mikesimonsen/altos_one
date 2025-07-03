import sqlite3
import pandas as pd

def main():
    db_path = 'altos_one.db'
    conn = sqlite3.connect(db_path)

    # Prompt: only top 50 metros?
    top50_choice = input("Include only top50 metros? (y/n, default n): ").strip().lower()
    filter_top50 = (top50_choice == 'y')

    # Load solds table
    df = pd.read_sql_query("SELECT * FROM solds", conn)
    # Process sold_date -> sold_month
    df['sold_date'] = pd.to_datetime(df['sold_date'], errors='coerce')
    df['sold_month'] = df['sold_date'].dt.strftime('%Y-%m')
    # Ensure numeric
    df['sold_price'] = pd.to_numeric(df['sold_price'], errors='coerce')
    df['list_price_final'] = pd.to_numeric(df['list_price_final'], errors='coerce')

    # Load metro mapping with display_name
    mapping = pd.read_sql_query("SELECT zipcode, metro, display_name FROM zip_to_metro", conn)
    conn.close()
    # Merge to add metro and display_name
    df = pd.merge(df, mapping, left_on='zip', right_on='zipcode', how='left')
    df.drop(columns=['zipcode'], inplace=True)
    df['display_name'] = df['display_name'].fillna('')

    # Optional: filter to top50 metros by display_name not blank
    if filter_top50:
        df = df[df['display_name'].astype(bool)]

    # Compute list_to_sale ratio per row
    def calc_ratio(row):
        sp = row['sold_price']
        lpf = row['list_price_final']
        if pd.notnull(sp) and pd.notnull(lpf) and lpf != 0:
            return 1 + ((sp - lpf) / lpf)
        return None
    df['sale_to_list_ratio'] = df.apply(calc_ratio, axis=1)

    # Group by sold_month, display_name (or metro if display blank), and type
    df['market_name'] = df.apply(
        lambda r: r['display_name'] if r['display_name'] else r['metro'], axis=1
    )
    grouped = df.groupby(['sold_month', 'market_name', 'type']).agg(
        sold_count=('sold_price', 'size'),
        median_sold_price=('sold_price', 'median'),
        average_sale_to_list_ratio=('sale_to_list_ratio', 'mean')
    ).reset_index()

    # Output to CSV
    default_out = 'solds_summary.csv'
    out_file = input(f"Enter output CSV filename (default {default_out}): ").strip() or default_out
    grouped.to_csv(out_file, index=False)
    print(f"✅ Exported summary to '{out_file}' with {len(grouped)} rows.")

if __name__ == '__main__':
    main()
