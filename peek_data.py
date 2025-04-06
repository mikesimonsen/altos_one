import sqlite3
import pandas as pd

def export_sample_to_csv(table_name, db_name='altos_one.db', sample_size=5):
    # Connect to database
    conn = sqlite3.connect(db_name)
    
    # Read sample data
    df_sample = pd.read_sql_query(f"SELECT * FROM {table_name} LIMIT {sample_size}", conn)
    
    # Save to CSV
    output_file = f"{table_name}_sample.csv"
    df_sample.to_csv(output_file, index=False)
    print(f"Sample data from '{table_name}' written to '{output_file}'.")
    
    conn.close()

def main():
    tables = ['listings', 'pendings']
    
    for table in tables:
        export_sample_to_csv(table)

if __name__ == "__main__":
    main()
