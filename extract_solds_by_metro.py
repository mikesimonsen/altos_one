import sqlite3
import pandas as pd

def extract_solds_by_metro(db_name='altos_one.db', metro_filter=""):
    """
    Connects to the database and retrieves all sold records joined with the zip_to_metro mapping,
    filtering for rows where the metro column (from the mapping table) contains metro_filter (case-insensitive).
    It then computes a new column, sale_to_list_price_ratio, using the formula:
    
         1 + ((sold_price - list_price_final) / list_price_final)
    
    The ratio is computed only if both sold_price and list_price_final are not null and list_price_final is not zero.
    """
    conn = sqlite3.connect(db_name)
    query = """
    SELECT s.*, z.metro
    FROM solds s
    LEFT JOIN zip_to_metro z ON s.zip = z.zipcode
    WHERE LOWER(z.metro) LIKE LOWER(?)
    """
    param = f"%{metro_filter}%"
    df = pd.read_sql_query(query, conn, params=(param,))
    conn.close()

    # Compute the sale_to_list_price_ratio column.
    def calc_ratio(row):
        try:
            sp = float(row['sold_price'])
            lpf = float(row['list_price_final'])
            if pd.notnull(sp) and pd.notnull(lpf) and lpf != 0:
                return 1 + ((sp - lpf) / lpf)
            else:
                return None
        except (ValueError, TypeError):
            return None

    df['sale_to_list_price_ratio'] = df.apply(calc_ratio, axis=1)
    return df

def main():
    metro_input = input("Enter the metro market name (or part of it) to filter sold properties: ").strip()
    if not metro_input:
        print("No metro market entered. Exiting.")
        return

    print(f"Searching for sold properties in metro markets containing '{metro_input}'...")
    df = extract_solds_by_metro(metro_filter=metro_input)
    
    if df.empty:
        print(f"No sold records found for metro matching '{metro_input}'.")
    else:
        # Create a safe output filename by replacing spaces with underscores.
        safe_metro = metro_input.replace(" ", "_")
        output_file = f"sold_properties_{safe_metro}.csv"
        df.to_csv(output_file, index=False)
        print(f"Extracted {len(df)} sold records for metro matching '{metro_input}' into {output_file}")

if __name__ == "__main__":
    main()
