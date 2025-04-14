import sqlite3
from datetime import datetime

def delete_rows_for_week(table_name, delete_date, db_name='altos_one.db'):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Count the rows that will be deleted
    cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE date = ?", (delete_date,))
    count = cursor.fetchone()[0]
    print(f"Found {count} rows in '{table_name}' with date = {delete_date}.")
    
    if count > 0:
        # Delete the rows
        cursor.execute(f"DELETE FROM {table_name} WHERE date = ?", (delete_date,))
        conn.commit()
        print(f"Deleted {cursor.rowcount} rows from '{table_name}'.")
    else:
        print("No rows to delete.")
    
    conn.close()

def main():
    table_name = input("Enter table name (listings or pendings): ").strip().lower()
    if table_name not in ["listings", "pendings"]:
        print("Invalid table name. Please enter 'listings' or 'pendings'.")
        return

    delete_date = input("Enter the date (YYYY-MM-DD) for which to delete rows: ").strip()
    # Validate date format
    try:
        datetime.strptime(delete_date, '%Y-%m-%d')
    except ValueError:
        print("Invalid date format. Please use YYYY-MM-DD.")
        return

    confirm = input(f"Are you sure you want to delete all rows from '{table_name}' where date = {delete_date}? (y/n): ").strip().lower()
    if confirm == 'y':
        delete_rows_for_week(table_name, delete_date)
    else:
        print("Deletion canceled.")

if __name__ == "__main__":
    main()
    