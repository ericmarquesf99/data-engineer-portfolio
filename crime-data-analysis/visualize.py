import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sqlite3
import os

def visualize_crime_data(db_path):
    """
    Generates visualizations of crime data.
    """
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql('SELECT * FROM crime_stats', conn)
        conn.close()

        # Bar chart: Crimes by location
        plt.figure(figsize=(10, 6))
        sns.barplot(data=df, x='DESCRICAOLOCAL', y='COUNT')
        plt.title('Crimes by Location')
        plt.xlabel('Location')
        plt.ylabel('Number of Crimes')
        plt.xticks(rotation=45)
        plt.savefig('data/processed/crime_by_location.png')
        plt.close()  # Close without showing

        # Another visualization: Temporal trend
        plt.figure()
        df['MES'] = pd.to_datetime(df['MES'].astype(str))
        df.groupby('MES')['COUNT'].sum().plot()
        plt.title('Crime Trends Over Time')
        plt.xlabel('Month')
        plt.ylabel('Total Crimes')
        plt.savefig('data/processed/crime_trend.png')
        plt.close()

    except Exception as e:
        print(f"Error in visualization: {e}")

# Example usage
if __name__ == "__main__":
    db_path = 'data/processed/crime_data.db'
    visualize_crime_data(db_path)