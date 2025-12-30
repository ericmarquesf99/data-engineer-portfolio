import pandas as pd
import requests
import zipfile
import io
import os

def extract_crime_data(url, output_path):
    """
    Extracts crime data from a URL (e.g., SSP-SP) and saves as CSV.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        # If it's a ZIP file, unzip it
        if 'zip' in url:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Assume there's a CSV inside
                for file in z.namelist():
                    if file.endswith('.csv'):
                        z.extract(file, output_path)
                        return os.path.join(output_path, file)

        # If it's a direct CSV
        with open(os.path.join(output_path, 'crime_data.csv'), 'wb') as f:
            f.write(response.content)
        return os.path.join(output_path, 'crime_data.csv')

    except Exception as e:
        print(f"Error extracting data: {e}")
        return None

# Example usage (replace with real SSP-SP URL)
# url = 'https://www.ssp.sp.gov.br/transparenciassp/Downloads/2023/DadosBO_2023_1.zip'
# file_path = extract_crime_data(url, 'data/raw/')