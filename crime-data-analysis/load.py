import pandas as pd
import sqlite3
import os

def load_crime_data_to_db(processed_file, db_path):
    """
    Carrega dados processados em um banco SQLite.
    """
    try:
        df = pd.read_csv(processed_file)

        conn = sqlite3.connect(db_path)
        df.to_sql('crime_stats', conn, if_exists='replace', index=False)
        conn.close()

        print(f"Dados carregados em {db_path}")
        return True

    except Exception as e:
        print(f"Erro no carregamento: {e}")
        return False

# Exemplo de uso
# processed_file = 'data/processed/processed_crime_data.csv'
# db_path = 'data/processed/crime_data.db'
# load_crime_data_to_db(processed_file, db_path)