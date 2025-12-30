import pandas as pd
import sqlite3
import os

def load_crime_data_to_db(processed_file, db_path):
    """
    Loads processed data into an SQLite database.
    """
    try:
        df = pd.read_csv(processed_file)

        conn = sqlite3.connect(db_path)
        df.to_sql('crime_stats', conn, if_exists='replace', index=False)
        conn.close()

        print(f"Data loaded into {db_path}")
        return True

    except Exception as e:
        print(f"Error in loading: {e}")
        return False

# Example usage
# processed_file = 'data/processed/processed_crime_data.csv'
# db_path = 'data/processed/crime_data.db'
# load_crime_data_to_db(processed_file, db_path)