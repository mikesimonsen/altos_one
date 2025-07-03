import sqlite3

def check_table_counts(db_name='altos_one.db'):
    """
    Print total counts and counts per date for each of the tables:
    'listings', 'pendings', and 'solds'. Assumes each table has columns 'date' and 'type'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    tables = ['listings', 'pendings', 'solds']
    
    for table in tables:
        print(f"Table '{table}':")
        
        # Total rows
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        total_rows = cursor.fetchone()[0]
        print(f"  Total rows: {total_rows}")
        
        # Total rows where type is "single_family"
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE type = 'single_family'")
        single_family_rows = cursor.fetchone()[0]
        print(f"  Rows with type='single_family': {single_family_rows}")
        
        # Counts per date: group by date column (YYYY-MM-DD)
        print("  Counts per date:")
        cursor.execute(
            f"""
            SELECT 
                date(date) AS day,
                COUNT(*) AS total_day,
                SUM(CASE WHEN type = 'single_family' THEN 1 ELSE 0 END) AS single_family_day
            FROM {table}
            GROUP BY day
            ORDER BY day
            """
        )
        daily_counts = cursor.fetchall()
        for day, total_day, sf_day in daily_counts:
            print(f"    {day}: total={total_day}, single_family={sf_day}")
        
        print("")
    
    conn.close()


def check_most_recent_dates(db_name='altos_one.db'):
    """
    Identify and print the most recent date in each of the tables:
    'listings', 'pendings', and 'solds'. Assumes each table has a column named 'date'.
    """
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    tables = ['listings', 'pendings', 'solds']
    
    for table in tables:
        # Retrieve the maximum (most recent) value in the 'date' column
        cursor.execute(f"SELECT MAX(date) FROM {table}")
        most_recent = cursor.fetchone()[0]
        
        print(f"Table '{table}' - Most recent date: {most_recent}")
    
    conn.close()

if __name__ == "__main__":
    check_table_counts()
    check_most_recent_dates()