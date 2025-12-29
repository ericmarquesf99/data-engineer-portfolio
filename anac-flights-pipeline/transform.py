import pandas as pd

def transform_flights_data(df):
    """
    Transforms the extracted data: cleans, converts types, and adds columns.
    Assumes standard columns from the OpenSky API.
    """
    # Select columns by index (assuming standard order)
    # 0: icao24, 1: callsign, 2: origin_country, 5: longitude, 6: latitude, 7: baro_altitude, 8: on_ground, 9: velocity
    df_selected = df.iloc[:, [0,1,2,5,6,7,8,9]].copy()
    df_selected.columns = ['aircraft_icao', 'flight_code', 'origin_country', 'longitude', 'latitude', 'altitude', 'on_ground', 'velocity']

    # Convert types
    df_selected['on_ground'] = df_selected['on_ground'].astype(bool)
    df_selected['altitude'] = pd.to_numeric(df_selected['altitude'], errors='coerce')
    df_selected['velocity'] = pd.to_numeric(df_selected['velocity'], errors='coerce')

    # Handle missing data
    df_selected.dropna(subset=['flight_code'], inplace=True)

    # Add status column
    df_selected['status'] = df_selected['on_ground'].map({True: 'on_ground', False: 'in_flight'})

    return df_selected

if __name__ == "__main__":
    df_raw = pd.read_csv('data/raw/flights_raw.csv')
    df_transformed = transform_flights_data(df_raw)
    print(f"Transformed data: {len(df_transformed)} flights")
    df_transformed.to_csv('data/processed/flights_transformed.csv', index=False)