# 📚 Documentation Index

Welcome to the Enterprise Data Pipeline documentation.

## 📖 Quick Links

### Getting Started
- [Setup Guide](SETUP.md) - Complete setup instructions
- [Databricks Guide](DATABRICKS_GUIDE.md) - Databricks configuration
- [Snowflake Setup](SNOWFLAKE_SETUP.md) - Snowflake configuration
- [Snowflake Checklist](SNOWFLAKE_CHECKLIST.md) - Pre-deployment checklist

### Architecture & Design
- [Architecture Overview](ARCHITECTURE.md) - System architecture and design patterns
- [Data Flow](DATA_FLOW.md) - Data flow and transformation logic
- [API Documentation](API.md) - API endpoints and usage

### Development
- [Development Guide](DEVELOPMENT.md) - Local development setup
- [Testing Guide](TESTING.md) - Running tests
- [Contributing Guidelines](CONTRIBUTING.md) - How to contribute

### Operations
- [Deployment Guide](DEPLOYMENT.md) - Production deployment
- [Monitoring](MONITORING.md) - Observability and monitoring
- [Troubleshooting](TROUBLESHOOTING.md) - Common issues and solutions

## 🏗️ Project Structure

```
enterprise-data-pipeline/
├── src/                    # Source code
│   ├── extractors/        # API extraction
│   ├── transformers/      # Data processing
│   ├── loaders/          # Data loading
│   └── utils/            # Utilities
├── notebooks/            # Databricks notebooks
├── tests/               # Test suites
├── config/              # Configuration files
├── sql/                 # SQL scripts
└── docs/                # Documentation (you are here)
```

## 🚀 Quick Start

1. **Install dependencies**: `pip install -r requirements.txt`
2. **Configure environment**: Copy `.env.example` to `.env`
3. **Run tests**: `pytest tests/`
4. **Deploy to Databricks**: Follow [Databricks Guide](DATABRICKS_GUIDE.md)

## 🔑 Key Concepts

### Medallion Architecture
- **Bronze Layer**: Raw data from APIs (DBFS)
- **Silver Layer**: Cleaned and validated data (Snowflake)
- **Gold Layer**: Aggregated metrics and insights (Snowflake)

### Technology Stack
- **Compute**: Databricks (Apache Spark)
- **Storage**: DBFS (Bronze), Snowflake (Silver/Gold)
- **Orchestration**: Databricks Jobs
- **Language**: Python 3.9+

## 📊 Data Sources
- **CoinGecko API v3**: Cryptocurrency market data

## 🔗 External Resources
- [Databricks Documentation](https://docs.databricks.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [CoinGecko API](https://www.coingecko.com/en/api/documentation)

## 📝 Version History
- **v1.0.0** (2026-01-19): Initial release with complete refactoring
  - Modular structure
  - Databricks notebooks
  - Comprehensive tests
  - Multi-environment config

## 🤝 Support
For questions or issues, please check the [Troubleshooting Guide](TROUBLESHOOTING.md) or open an issue.
