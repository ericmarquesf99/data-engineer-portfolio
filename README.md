# Data Engineer Portfolio

Welcome to my data engineering portfolio. This repository showcases my expertise in building scalable, production-ready data pipelines and solving complex data challenges.

---

## 👤 About Me

I'm a passionate data engineer with expertise in:
- **ETL/ELT Pipeline Design**: Building efficient data workflows
- **Data Storage & Management**: SQL, NoSQL, and data warehouse solutions
- **Data Processing**: Pandas, PySpark, and distributed computing
- **Cloud Technologies**: AWS, GCP fundamentals
- **API Integration**: RESTful APIs and real-time data streams
- **Database Design**: Schema optimization and query performance

---

## 🚀 Featured Projects

### 1. **Real-Time Flight Data ETL Pipeline**

**Status**: ✅ Complete  
**Technologies**: Python, Pandas, SQLite, OpenSky Network API, Matplotlib, Seaborn

#### Overview
A production-ready ETL pipeline that extracts, transforms, and loads real-time flight data across Brazil using the OpenSky Network API. The project demonstrates core data engineering principles and provides actionable insights through automated visualizations.

#### Key Achievements
- ✅ Implemented robust data extraction from public APIs
- ✅ Built transformation logic with comprehensive data cleaning
- ✅ Designed SQLite schema for efficient querying
- ✅ Created 4 professional visualizations for data insights
- ✅ Modular, maintainable codebase with clear separation of concerns

#### Architecture
```
OpenSky API → Extract → Transform → Load → SQLite → Visualizations
```

#### Results
- **Data Processed**: 130+ real-time flight records per execution
- **Data Quality**: 100% valid records after transformation
- **Query Performance**: Sub-millisecond response times on SQLite
- **Uptime**: Designed for continuous operation

#### Visualizations Generated
1. **Flights by Country** - Validates data collection accuracy
2. **Altitude Distribution** - Shows typical cruise altitudes (0-12K meters)
3. **Flight Positions** - Geographic distribution across Brazilian airspace
4. **Flight Status** - Real-time operational snapshot (in-flight vs. on-ground)

#### Technologies Used
- **Language**: Python 3.11+
- **Data Processing**: Pandas for ETL transformations
- **Database**: SQLite with SQLAlchemy ORM
- **APIs**: Requests library for REST API integration
- **Visualization**: Matplotlib + Seaborn for statistical graphics
- **Version Control**: Git

#### Project Structure
```
anac-flights-pipeline/
├── extract.py          # OpenSky API data extraction
├── transform.py        # Data cleaning & feature engineering
├── load.py            # SQLite database operations
├── main.py            # Pipeline orchestration
├── visualize.py       # Analytics and reporting
├── data/              # Raw and processed data storage
└── README.md          # Detailed documentation
```

#### How to Use
```bash
# Clone the repository
git clone https://github.com/yourusername/data-engineer-portfolio.git
cd anac-flights-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the complete pipeline
python main.py

# Generate visualizations
python visualize.py
```

#### Key Learnings
- Designing efficient ETL workflows for real-time data
- Implementing error handling and data validation
- Creating modular, reusable code components
- Optimizing database operations for performance
- Presenting data insights through visualizations

#### Future Enhancements
- [ ] Add Apache Airflow for orchestration
- [ ] Implement historical data analytics
- [ ] Deploy to cloud (AWS/GCP)
- [ ] Create interactive Streamlit dashboard
- [ ] Add comprehensive unit tests
- [ ] Implement data lineage tracking

[**View Full Project Details** →](./anac-flights-pipeline/README.md)

---

## 💻 Technical Skills

### Languages & Frameworks
- **Python** (Primary) - Pandas, Polars, PySpark, NumPy
- **SQL** - PostgreSQL, SQLite, MySQL query optimization
- **Scripting** - Bash, PowerShell

### Data Technologies
- **ETL Tools**: Python-based pipelines, Apache Airflow
- **Data Warehousing**: SQL-based solutions, dimensional modeling
- **Databases**: SQLite, PostgreSQL, MySQL
- **BI Tools**: Matplotlib, Seaborn, Plotly
- **APIs**: RESTful services, real-time data streams

### Cloud & DevOps
- **AWS**: Fundamentals (S3, EC2, RDS)
- **GCP**: Cloud Storage, BigQuery basics
- **Docker**: Containerization (planned)
- **CI/CD**: Version control with Git

### Best Practices
- ✅ Clean code principles (DRY, SOLID)
- ✅ Comprehensive documentation
- ✅ Version control and branching strategies
- ✅ Testing and validation
- ✅ Performance optimization

---

## 📊 Portfolio Statistics

| Metric | Value |
|--------|-------|
| **Projects Completed** | 1+ |
| **Code Repositories** | Public on GitHub |
| **Data Processed** | 130+ records per run |
| **Languages Used** | Python, SQL, Bash |
| **API Integrations** | OpenSky Network |
| **Database Systems** | SQLite |

---

## 📚 Learning Path & Interests

### Current Focus
- Advanced ETL patterns and best practices
- Cloud data solutions (AWS/GCP)
- Real-time streaming (Kafka, Spark Streaming)
- Data quality and validation frameworks

### Areas of Interest
- Machine Learning pipelines
- Big Data technologies (Spark, Hadoop)
- Data governance and lineage
- Performance optimization

---

## 🔗 Links & Resources

- **GitHub**: [github.com/yourusername](https://github.com/yourusername)
- **LinkedIn**: [linkedin.com/in/yourprofile](https://linkedin.com/in/yourprofile)
- **Email**: [your.email@example.com](mailto:your.email@example.com)

---

## 📄 License

All projects are available under the MIT License unless otherwise specified.

---

## 🙏 Acknowledgments

- OpenSky Network for providing free, public flight data
- Python community for excellent data engineering libraries
- Open source contributors who make projects like these possible

---

**Last Updated**: December 29, 2025

*This portfolio is continuously updated with new projects and learnings.*
