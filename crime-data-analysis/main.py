from transform import transform_crime_data
from load import load_crime_data_to_db
import os

def main():
    """
    Pipeline completo: Extract, Transform, Load.
    """
    # Para teste, usar arquivo local
    raw_file = 'data/raw/crime_data.csv'
    processed_dir = 'data/processed/'
    db_path = os.path.join(processed_dir, 'crime_data.db')

    if os.path.exists(raw_file):
        processed_file = transform_crime_data(raw_file, processed_dir)
        if processed_file:
            load_crime_data_to_db(processed_file, db_path)

    print("Pipeline concluído!")

if __name__ == "__main__":
    main()