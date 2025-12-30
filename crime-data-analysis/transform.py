import pandas as pd
import os

def transform_crime_data(input_path, output_path):
    """
    Transforma dados de crimes: limpeza, normalização e agregação.
    """
    try:
        df = pd.read_csv(input_path, encoding='latin1', sep=';')  # Ajustar separador se necessário

        # Limpeza básica
        df.dropna(subset=['DATAOCORRENCIA', 'DESCRICAOLOCAL'], inplace=True)  # Exemplo de colunas

        # Normalizar datas
        df['DATAOCORRENCIA'] = pd.to_datetime(df['DATAOCORRENCIA'], errors='coerce')

        # Agregar por bairro e mês
        df['MES'] = df['DATAOCORRENCIA'].dt.to_period('M')
        aggregated = df.groupby(['DESCRICAOLOCAL', 'MES']).size().reset_index(name='COUNT')

        # Salvar dados processados
        processed_file = os.path.join(output_path, 'processed_crime_data.csv')
        aggregated.to_csv(processed_file, index=False)
        return processed_file

    except Exception as e:
        print(f"Erro na transformação: {e}")
        return None

# Exemplo de uso
# input_file = 'data/raw/crime_data.csv'
# output_file = transform_crime_data(input_file, 'data/processed/')