import sqlite3
import pandas as pd

def export_table_schema(table_name, db_name='altos_one.db'):
    # Connect to the database
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Get schema info
    cursor.execute(f"PRAGMA table_info({table_name})")
    schema_info = cursor.fetchall()
    
    # Build a DataFrame
    schema_df = pd.DataFrame(schema_info, columns=[
        'cid', 'name', 'type', 'notnull', 'default_value', 'primary_key'
    ])
    
    # Save schema to CSV
    output_file = f"{table_name}_schema.csv"
    schema_df.to_csv(output_file, index=False)
    print(f"Schema for '{table_name}' written to '{output_file}'.")
    
    conn.close()

def main():
    tables = ['listings', 'pendings']
    
    for table in tables:
        export_table_schema(table)

if __name__ == "__main__":
    main()
