import pandas as pd
import os

def transform_crime_data(input_path, output_path):
    """
    Transforms crime data: cleaning, normalization, and aggregation.
    """
    try:
        df = pd.read_csv(input_path, encoding='latin1', sep=';')  # Adjust separator if necessary

        # Basic cleaning
        df.dropna(subset=['DATAOCORRENCIA', 'DESCRICAOLOCAL'], inplace=True)  # Example columns

        # Normalize dates
        df['DATAOCORRENCIA'] = pd.to_datetime(df['DATAOCORRENCIA'], errors='coerce')

        # Aggregate by location and month
        df['MES'] = df['DATAOCORRENCIA'].dt.to_period('M')
        aggregated = df.groupby(['DESCRICAOLOCAL', 'MES']).size().reset_index(name='COUNT')

        # Save processed data
        processed_file = os.path.join(output_path, 'processed_crime_data.csv')
        aggregated.to_csv(processed_file, index=False)
        return processed_file

    except Exception as e:
        print(f"Error in transformation: {e}")
        return None

# Example usage
# input_file = 'data/raw/crime_data.csv'
# output_file = transform_crime_data(input_file, 'data/processed/')