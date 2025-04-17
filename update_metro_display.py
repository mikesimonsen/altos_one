import sqlite3
import pandas as pd

METROS_CSV = 'metros_msa.csv'  # Path to your 50-metro mapping CSV
DB_PATH     = 'altos_one.db'  # Path to your SQLite database


def update_metro_display(db_path=DB_PATH, csv_path=METROS_CSV):
    """
    1) Alters zip_to_metro to add a display_name column (if it doesn't already exist).
    2) Reads a CSV of the top 50 MSAs with columns 'MSA name' (the key) and 'display name'.
    3) Updates zip_to_metro.display_name for any row whose metro value contains the MSA name.
    """
    # Load the CSV mapping
    df = pd.read_csv(csv_path)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1) Add display_name column if missing
    try:
        cursor.execute("ALTER TABLE zip_to_metro ADD COLUMN display_name TEXT;")
        print("Added 'display_name' column to zip_to_metro.")
    except sqlite3.OperationalError:
        # Column likely already exists
        print("'display_name' column already exists; skipping ALTER TABLE.")

    # 2) For each MSA mapping, update matching rows
    for _, row in df.iterrows():
        msa_key    = row['MSA name']
        disp_name  = row['display name']
        # Update any zip_to_metro.metro that contains the MSA key
        sql = """
        UPDATE zip_to_metro
        SET display_name = ?
        WHERE metro LIKE ?;
        """
        cursor.execute(sql, (disp_name, f"%{msa_key}%"))
        print(f"Mapped metro '%{msa_key}%' → '{disp_name}' ({cursor.rowcount} rows updated)")

    conn.commit()
    conn.close()
    print("✅ Top‑50 MSA display names updated.")


if __name__ == '__main__':
    update_metro_display()
