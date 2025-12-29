import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine

# Set style for better visuals
sns.set(style="whitegrid")

def load_data_from_db(db_path='data/processed/flights.db', table_name='flights'):
    """
    Loads data from the SQLite database.
    """
    engine = create_engine(f'sqlite:///{db_path}')
    df = pd.read_sql(f'SELECT * FROM {table_name}', engine)
    return df

def create_visualizations(df):
    """
    Creates basic visualizations from the flight data.
    Saves plots as PNG files.
    """
    # 1. Bar chart: Number of flights by origin_country (should be mostly Brazil)
    plt.figure(figsize=(8, 5))
    df['origin_country'].value_counts().plot(kind='bar', color='skyblue')
    plt.title('Number of Flights by Origin Country')
    plt.xlabel('Origin Country')
    plt.ylabel('Number of Flights')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig('data/processed/flights_by_country.png')
    plt.show()

    # 2. Histogram: Distribution of altitude
    plt.figure(figsize=(8, 5))
    df['altitude'].dropna().plot(kind='hist', bins=20, color='green', edgecolor='black')
    plt.title('Distribution of Flight Altitudes')
    plt.xlabel('Altitude (meters)')
    plt.ylabel('Frequency')
    plt.tight_layout()
    plt.savefig('data/processed/altitude_distribution.png')
    plt.show()

    # 3. Scatter plot: Longitude vs Latitude (flight positions)
    plt.figure(figsize=(8, 6))
    plt.scatter(df['longitude'], df['latitude'], alpha=0.5, color='red')
    plt.title('Flight Positions (Longitude vs Latitude)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.grid(True)
    plt.tight_layout()
    plt.savefig('data/processed/flight_positions.png')
    plt.show()

    # 4. Bar chart: Flights by status (on_ground vs in_flight)
    plt.figure(figsize=(8, 5))
    df['status'].value_counts().plot(kind='bar', color='orange')
    plt.title('Number of Flights by Status')
    plt.xlabel('Status')
    plt.ylabel('Number of Flights')
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig('data/processed/flights_by_status.png')
    plt.show()

if __name__ == "__main__":
    df = load_data_from_db()
    print(f"Loaded {len(df)} records from database.")
    create_visualizations(df)
    print("Visualizations created and saved as PNG files in data/processed/")