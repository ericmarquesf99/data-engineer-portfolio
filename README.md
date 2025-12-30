# Eric Ferreira - Data Engineer Portfolio

Welcome to my data engineering portfolio. This repository showcases my expertise in building scalable, production-ready data pipelines and solving complex data challenges.

---

## 👤 About Me

**Eric Ferreira**  
**Data Engineer**  
**Location**: Fortaleza, Ceará, Brazil  
**Phone**: +55 85 98509-7717  
**Email**: [ericmarques1999@email.com](mailto:ericmarques1999@email.com)  
**LinkedIn**: [https://www.linkedin.com/in/eric-marquesf/](https://www.linkedin.com/in/eric-marquesf/)

### Summary
Data engineer with over 3 years of diverse experience and over 7 years working in technology related roles. Known for demonstrating a keen eye for extracting, treating and loading data in order to enable data driven decisions. Expertise in leveraging key technologies such as SQL, Python, and Azure to take the best from any kind of data and increase the data science effectiveness.

---

## 💼 Professional Experience

### Data Engineer
**KIS, United States – Remote**  
*01/2025 to present*

**Core Responsibilities:**
- Design and maintain data pipelines to ensure the data is always available for business users and data analysts
- Apply enhancements to existing data pipelines to reduce computing cost for the business
- Create documentation of new data pipelines to ensure
- Extract data from multiple sources such as APIs, raw csv files, json files, parquet files
- Create data pipelines in Azure Data Factory to automate data ingestion and transformation scripts to make fresh data available 50% faster
- Create Python and PySpark scripts using Databricks to transform data and meet highly complex business requirements
- Perform ad-hoc data analysis on SQL Server and MySQL databases to support business data decisions

**Key Technologies and Tools:** SQL, Snowflake, Python, Databricks, PySpark, Git, Agile, Azure Cloud, Azure Data Factory.

### Data Engineer
**Dexian, Curitiba, BR – Remote**  
*08/2023 to 12/2024*

**Core Responsibilities:**
- Created complex views in Snowflake using SQL achieving better data treatment and enabling better data analysis and data science
- Materialized views using SQL and JavaScript in order to make the view's performance 70% faster
- Created data pipelines in Azure Data Factory to automate data ingestion and transformation scripts to make fresh data available 50% faster
- Created Python and PySpark scripts using Databricks to transform data and meet highly complex business requirements
- Followed CI/CD development management guidelines using GitHub in order to deploy high quality and sustainable code enabling much more collaboration between development teams

**Key Technologies and Tools:** SQL, Snowflake, Python, Databricks, PySpark, Git, Agile, Azure Cloud, Azure Data Factory.

### Data Engineer
**Curitiba, Fortaleza, BR – Remote**  
*01/2022 to 07/2023*

**Core Responsibilities:**
- Created complex views in Snowflake using SQL achieving better data treatment and enabling better data analysis and data science
- Created .csv loading scripts using Python in order to load huge datasets into Data Warehouse tables
- Created data pipelines in Azure Data Factory to automate data ingestion and transformation scripts to make fresh data available 50% faster

**Key Technologies and Tools:** SQL, Snowflake, Python, Git, Azure Data Factory, Retail Industry knowledge.

### Data Analyst
**LIQ, Fortaleza, BR – Remote**  
*06/2021 to 12/2021*

**Core Responsibilities:**
- Developed complex queries on large data sets using SQL to improve Excel analytics reports
- Created managerial reports in Excel, to analyze the productivity of the call center teams enabling data driven decisions
- Developed complex formulas in Excel, to meet business requirements

**Key Technologies and Tools:** SQL, SQL Server, Excel, VBA, Google Data Studio.

### Support Analyst
**Dragão dos Parafusos, Fortaleza, BR**  
*09/2020 to 07/2021*

**Core Responsibilities:**
- Created SQL queries for development of management reports to enable data driven decisions
- Timely fixed any bugs that might make the sales operations go down
- Developed solutions to streamline the company's processes within the system

**Key Technologies and Tools:** SQL, ERP, Oracle, Excel, PL/SQL

### Support Analyst
**Comercial Brasil, Fortaleza, BR**  
*10/2016 to 07/2021*

**Core Responsibilities:**
- Created SQL queries for development of management reports to enable data driven decisions
- Timely fixed any bugs that might make the sales operations go down
- Developed solutions to streamline the company's processes within the system

**Key Technologies and Tools:** SQL, ERP, Oracle, Excel, PL/SQL

---

## 🎓 Education

**Bachelor of System Analysis**  
Estácio de Sá, Ceará, Brazil  
*01/2018 to 12/2020*

---

## 🌐 Languages

- **Portuguese**: Native
- **English**: Fluent

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

### Proficient
- SQL, Data Engineering, Oracle, Snowflake, ETL, Excel, Data Warehouse, Database Development, Data Integration

### Intermediate
- Databricks, Azure Cloud, Azure Data Factory, Python, Git, GitHub, Agile Development, SQL Server, MySQL

### Beginner
- React, Kafka

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
- **LinkedIn**: [https://www.linkedin.com/in/eric-marquesf/](https://www.linkedin.com/in/eric-marquesf/)
- **Email**: [ericmarques1999@email.com](mailto:ericmarques1999@email.com)

---

## 📄 License

All projects are available under the MIT License unless otherwise specified.

---

## 🙏 Acknowledgments

- OpenSky Network for providing free, public flight data
- Python community for excellent data engineering libraries
- Open source contributors who make projects like these possible

---

**Last Updated**: December 30, 2025

*This portfolio is continuously updated with new projects and learnings.*
