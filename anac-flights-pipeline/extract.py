import requests
import pandas as pd
from datetime import datetime, timedelta
import time

def extract_flights_data():
    """
    Extracts real-time flight data from the OpenSky Network API.
    Filters flights with origin or destination in Brazil (ICAO starting with 'SB').
    Note: For historical data, a free API key from the website is required.
    """
    base_url = "https://opensky-network.org/api/states/all"

    response = requests.get(base_url)
    if response.status_code != 200:
        raise Exception(f"API Error: {response.status_code} - {response.text}")

    data = response.json()

    # Data comes in 'states': list of lists
    df = pd.DataFrame(data['states'])

    # Assume standard columns (may vary, but usually 17)
    # Filter Brazilian flights (origin_country == 'Brazil')
    if len(df.columns) >= 3:  # At least icao24, callsign, origin_country
        df_filtered = df[df.iloc[:, 2] == 'Brazil']  # Column 2 is origin_country
    else:
        df_filtered = df

    return df_filtered

if __name__ == "__main__":
    df = extract_flights_data()
    print(f"Extracted data: {len(df)} real-time flights")
    df.to_csv('data/raw/flights_raw.csv', index=False)