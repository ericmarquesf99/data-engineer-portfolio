import pandas as pd
import requests
import zipfile
import io
import os

def extract_crime_data(url, output_path):
    """
    Extrai dados de crimes de uma URL (ex: SSP-SP) e salva como CSV.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()

        # Se for ZIP, descompactar
        if 'zip' in url:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                # Assumir que há um CSV dentro
                for file in z.namelist():
                    if file.endswith('.csv'):
                        z.extract(file, output_path)
                        return os.path.join(output_path, file)

        # Se for CSV direto
        with open(os.path.join(output_path, 'crime_data.csv'), 'wb') as f:
            f.write(response.content)
        return os.path.join(output_path, 'crime_data.csv')

    except Exception as e:
        print(f"Erro ao extrair dados: {e}")
        return None

# Exemplo de uso (substitua pela URL real do SSP-SP)
# url = 'https://www.ssp.sp.gov.br/transparenciassp/Downloads/2023/DadosBO_2023_1.zip'
# file_path = extract_crime_data(url, 'data/raw/')