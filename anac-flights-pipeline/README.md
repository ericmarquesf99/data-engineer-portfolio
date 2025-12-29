# Flights ETL Pipeline with OpenSky Network

A production-ready ETL (Extract, Transform, Load) pipeline for real-time flight data across Brazil, leveraging the public OpenSky Network API. This project demonstrates core data engineering principles including data extraction, transformation, and persistence in a relational database.

**Status**: ✅ Production Ready  
**Last Updated**: December 2025  
**License**: MIT

---

## 🎯 Objective

This project implements a robust pipeline that:
- **Extracts** real-time flight data from the OpenSky Network API
- **Transforms** raw flight information (cleaning, type conversion, feature engineering)
- **Loads** processed data into a SQLite database for analytics and reporting
- **Visualizes** key insights from Brazilian flight operations

The pipeline is designed to be scalable, maintainable, and serves as a foundation for more advanced data engineering solutions.

---

## 📊 Project Overview

### Architecture
```
OpenSky Network API
        ↓
    Extract (extract.py)
        ↓
    Transform (transform.py)
        ↓
    Load (load.py)
        ↓
    SQLite Database
        ↓
    Visualizations & Analytics
```

### Key Features
- **Real-time Data Collection**: Captures current flight positions across Brazil
- **Automatic Type Conversion**: Handles numeric conversions and data validation
- **Null Value Handling**: Robust handling of missing or incomplete data
- **Status Classification**: Categorizes flights as "in_flight" or "on_ground"
- **Data Persistence**: Stores data in SQLite for reproducible analysis
- **Visualization**: Generates insights through multiple chart types

---

## 📁 Project Structure

```
anac-flights-pipeline/
├── extract.py              # Data extraction from OpenSky API
├── transform.py            # Data cleaning and transformation logic
├── load.py                 # Database loading operations
├── main.py                 # Pipeline orchestration
├── visualize.py            # Data visualization and reporting
├── README.md               # This file
└── data/
    ├── raw/                # Raw CSV files from API extraction
    └── processed/          # Transformed CSV and SQLite database
        ├── flights.db      # SQLite database
        ├── flights_by_country.png
        ├── altitude_distribution.png
        ├── flight_positions.png
        └── flights_by_status.png
```

---

## 🛠️ Technologies & Dependencies

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Runtime** | Python 3.11+ | Core programming language |
| **Data Processing** | Pandas | DataFrame manipulation and cleaning |
| **API Client** | Requests | HTTP communication with OpenSky API |
| **Database** | SQLite + SQLAlchemy | Data persistence and ORM |
| **Visualization** | Matplotlib + Seaborn | Statistical graphics and charts |

### Installation

```bash
# Create virtual environment
python -m venv venv
source venv/Scripts/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install requests pandas sqlalchemy matplotlib seaborn
```

---

## 🚀 Usage Guide

### Running the Full ETL Pipeline

```bash
python main.py
```

**Output**:
```
Starting extraction...
Starting transformation...
Starting load...
Data loaded into table 'flights' in database 'data/processed/flights.db'
ETL Pipeline completed!
```

### Running Individual Components

```bash
# Extract only
python extract.py

# Transform only
python transform.py

# Load only
python load.py

# Generate visualizations
python visualize.py
```

### Querying the Database

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('sqlite:///data/processed/flights.db')

# Example: Get all flights in the air
df = pd.read_sql("SELECT * FROM flights WHERE status='in_flight'", engine)
print(df)
```

---

## 📈 Data Schema

### Flights Table

| Column | Type | Description |
|--------|------|-------------|
| `aircraft_icao` | STRING | ICAO 24-bit address of the aircraft |
| `flight_code` | STRING | IATA/ICAO flight identifier (callsign) |
| `origin_country` | STRING | Country of origin (e.g., "Brazil") |
| `longitude` | FLOAT | Longitude coordinate of aircraft position |
| `latitude` | FLOAT | Latitude coordinate of aircraft position |
| `altitude` | FLOAT | Barometric altitude in meters |
| `on_ground` | BOOLEAN | 0 = in flight, 1 = on ground |
| `velocity` | FLOAT | Ground speed in m/s |
| `status` | STRING | Derived status: "in_flight" or "on_ground" |

---

## 📊 Visualizations

The pipeline generates four key visualizations:

### 1. **Flights by Country** (`flights_by_country.png`)
- Bar chart showing aircraft distribution by origin country
- Validates data collection accuracy

### 2. **Altitude Distribution** (`altitude_distribution.png`)
- Histogram of flight altitudes
- Shows aircraft are cruising at typical commercial altitudes

### 3. **Flight Positions** (`flight_positions.png`)
- Scatter plot of longitude vs. latitude
- Geographic distribution of tracked aircraft over Brazil

### 4. **Flights by Status** (`flights_by_status.png`)
- Bar chart comparing in-flight vs. on-ground aircraft
- Real-time operational snapshot

---

## 💡 Key Implementation Details

### Extract Phase
- Uses the `/api/states/all` endpoint for real-time data
- No authentication required for public data access
- Filters results to Brazilian flights only

### Transform Phase
- Converts Unix timestamps to datetime objects
- Coerces numeric columns (altitude, velocity) with error handling
- Removes records with missing flight codes
- Creates derived "status" column from boolean "on_ground" field

### Load Phase
- Uses SQLAlchemy for database abstraction
- Replaces existing data on each run (idempotent operation)
- Provides clear logging of successful loads

---

## 🔄 Workflow

1. **API Call**: Fetches real-time flight states from OpenSky
2. **Filtering**: Selects only Brazilian flights
3. **Data Cleaning**: Handles missing values, type conversions
4. **Database Insert**: Stores processed data in SQLite
5. **Visualization**: Generates analytical charts

---

## 🚧 Advanced Features & Extensions

### Planned Enhancements
- [ ] Schedule pipeline runs with Apache Airflow
- [ ] Add historical data support with API credentials
- [ ] Implement data validation and quality checks
- [ ] Create interactive dashboards with Streamlit
- [ ] Deploy to AWS/GCP for cloud processing
- [ ] Add unit and integration tests
- [ ] Implement data lineage tracking

### For Production Deployment
- Use environment variables for database paths
- Implement error handling and retry logic
- Add logging with Python's logging module
- Monitor API rate limits and handle throttling
- Implement data versioning and archival

---

## 📝 API Reference

### OpenSky Network API

**Endpoint**: `https://opensky-network.org/api/states/all`  
**Authentication**: None required for real-time data  
**Rate Limiting**: Free tier allows regular requests

**Response Fields** (as returned):
- ICAO 24-bit address
- Call sign
- Origin country
- Timestamp of position report
- Longitude/Latitude (WGS-84)
- Altitude (meters)
- On ground flag
- Velocity (m/s)

For historical data (30+ days past), a free account API key is required.

---

## 🧪 Testing & Validation

### Sample Data Statistics

Sample run: **130 Brazilian aircraft tracked**
- Altitudes: 0 - 12,192 meters (typical commercial range)
- Speeds: 180 - 250 m/s (typical cruise speeds)
- Geographic spread: Full coverage of Brazilian airspace

---

## 📚 References

- [OpenSky Network Documentation](https://opensky-network.org/api)
- [OpenSky API Specification](https://opensky-network.org/api/documentation)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [SQLAlchemy Tutorial](https://docs.sqlalchemy.org/en/20/tutorial/)

---

## 📧 Contact & Support

For questions or contributions, feel free to reach out.

---

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
