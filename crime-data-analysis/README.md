# Crime Data Analysis Pipeline

An urban crime data analysis pipeline focused on São Paulo, Brazil. This project demonstrates intermediate data engineering skills, including extraction from public data sources, transformation, database loading, and visualizations.

## Overview

The project collects crime occurrence data from SSP-SP (São Paulo State Public Security Department), processes it to identify patterns by neighborhood and period, and generates insights through visualizations.

## Key Achievements
- Automated data extraction from public sources
- Data cleaning and aggregation for analysis
- Loading into SQLite database for efficient queries
- Heatmap and temporal trend visualizations

## Architecture
```
SSP-SP API/CSV → Extract → Transform → Load → SQLite → Visualizations
```

## Results
- Data Processed: 60+ crime records from January to March 2025
- Quality: Cleaned and aggregated data
- Performance: Fast queries on SQLite
- Insights: Crime patterns by neighborhood and month

## Technologies Used
- **Language**: Python 3.11+
- **Processing**: Pandas for ETL
- **Database**: SQLite
- **APIs**: Requests for downloads
- **Visualization**: Matplotlib + Seaborn

## Project Structure
```
crime-data-analysis/
├── extract.py          # SSP-SP data extraction
├── transform.py        # Data cleaning and transformation
├── load.py            # SQLite loading
├── main.py            # Pipeline orchestration
├── visualize.py       # Chart generation
├── data/              # Raw and processed data
└── README.md          # Documentation
```

## How to Use
1. Install dependencies: `pip install -r requirements.txt`
2. Download data manually if needed (or adjust URLs in extract.py)
3. Run the pipeline: `python main.py`
4. Generate visualizations: `python visualize.py`

## Key Learnings
- Handling geospatial data
- Temporal aggregation for insights
- Integration with government data sources

## Sample Visualizations
- **Crimes by Location**: ![Crimes by Location](crime_by_location.png)
- **Crime Trends Over Time**: ![Crime Trends Over Time](crime_trend.png)

## Future Enhancements
- Add interactive maps with Geopandas
- Implement clustering for hotspots
- Deploy on Azure for automation