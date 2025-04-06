import sqlite3

def check_table_counts(db_name='altos_one.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    tables = ['listings', 'pendings']
    
    for table in tables:
        # Count total rows
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
        
        # Count rows where type is "single_family"
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE type = 'single_family'")
        single_family_rows = cursor.fetchone()[0]
        
        print(f"Table '{table}':")
        print(f"  Total rows: {total_rows}")
        print(f"  Rows with type='single_family': {single_family_rows}\n")
    
    conn.close()

if __name__ == "__main__":
    check_table_counts()
