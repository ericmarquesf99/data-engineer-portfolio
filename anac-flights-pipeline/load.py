import pandas as pd
from sqlalchemy import create_engine

def load_flights_data(df, db_path='data/processed/flights.db', table_name='flights'):
    """
    Loads the transformed data into a SQLite database.
    """
    engine = create_engine(f'sqlite:///{db_path}')

    # Load DataFrame into table (replace if exists)
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    print(f"Data loaded into table '{table_name}' in database '{db_path}'")

if __name__ == "__main__":
    df_transformed = pd.read_csv('data/processed/flights_transformed.csv')
    load_flights_data(df_transformed)