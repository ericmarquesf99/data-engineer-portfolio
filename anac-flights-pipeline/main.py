from extract import extract_flights_data
from transform import transform_flights_data
from load import load_flights_data

def main():
    """
    Runs the complete ETL pipeline with real-time data.
    """
    print("Starting extraction...")
    df_raw = extract_flights_data()

    print("Starting transformation...")
    df_transformed = transform_flights_data(df_raw)

    print("Starting load...")
    load_flights_data(df_transformed)

    print("ETL Pipeline completed!")

if __name__ == "__main__":
    main()