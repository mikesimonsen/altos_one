import sqlite3
import pandas as pd

def analyze_solds_weeks_count(db_name='altos_one.db', target_date=None, output_file=None):
    # Connect to the database and load the solds table.
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT * FROM solds", conn)
    conn.close()
    
    # Group by the 'date' column and count non-null entries for 'listed_on' and 'pending_on'
    grouped = df.groupby('date').agg(
        listed_on_count = ('listed_on', lambda x: x.notnull().sum()),
        pending_on_count = ('pending_on', lambda x: x.notnull().sum())
    ).reset_index()
    
    # Get the target date from the user if not provided.
    if not target_date:
        target_date = input("Enter the target date (YYYY-MM-DD) for output file naming: ").strip()
    
    # Build default output filename if not provided.
    if not output_file:
        output_file = f"sold_weeks_count_{target_date}.csv"
    
    grouped.to_csv(output_file, index=False)
    print(f"Output saved to {output_file}")
    
def main():
    analyze_solds_weeks_count()

if __name__ == "__main__":
    main()
