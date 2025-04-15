import sqlite3
import pandas as pd

def analyze_solds_histograms(db_name='altos_one.db'):
    # Connect to the SQLite database and read the entire solds table.
    conn = sqlite3.connect(db_name)
    df = pd.read_sql_query("SELECT * FROM solds", conn)
    conn.close()
    
    # --- Group by the listed_on column ---
    # We first filter out any null or blank values.
    df_listed = df[df['listed_on'].notnull() & (df['listed_on'] != "")]
    listed_on_counts = df_listed.groupby('listed_on').size().reset_index(name='count')
    
    # --- Group by the pending_on column ---
    df_pending = df[df['pending_on'].notnull() & (df['pending_on'] != "")]
    pending_on_counts = df_pending.groupby('pending_on').size().reset_index(name='count')
    
    # Export results to CSV files.
    listed_on_counts.to_csv("solds_listed_on_histogram.csv", index=False)
    pending_on_counts.to_csv("solds_pending_on_histogram.csv", index=False)
    
    print("Exported histogram data:")
    print(" - solds_listed_on_histogram.csv")
    print(" - solds_pending_on_histogram.csv")

def main():
    analyze_solds_histograms()

if __name__ == "__main__":
    main()
