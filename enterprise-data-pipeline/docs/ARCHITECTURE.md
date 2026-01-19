# 🏗️ Architecture Overview

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                 │
│                  CoinGecko API v3                                │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   EXTRACTION LAYER                               │
│                  (Databricks Notebook)                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • API calls with retry logic                             │  │
│  │ • Rate limiting                                           │  │
│  │ • Error handling                                          │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (DBFS)                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Raw JSON files: dbfs:/mnt/data/bronze/crypto/            │  │
│  │ • Immutable raw data                                      │  │
│  │ • Timestamped files                                       │  │
│  │ • Full API responses                                      │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│               TRANSFORMATION LAYER (PySpark)                     │
│                  (Databricks Notebook)                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Schema validation                                       │  │
│  │ • Data cleaning & deduplication                           │  │
│  │ • Type conversions                                        │  │
│  │ • Quality checks                                          │  │
│  │ • Anomaly detection                                       │  │
│  │ • Aggregations                                            │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                SILVER LAYER (Snowflake)                          │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ silver_crypto_clean table                                │  │
│  │ • Cleaned & validated data                                │  │
│  │ • Type 2 SCD (Slowly Changing Dimensions)                 │  │
│  │ • Historical tracking                                     │  │
│  │ • is_current, valid_from, valid_to                        │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                GOLD LAYER (Snowflake)                            │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ gold_crypto_metrics table                                │  │
│  │ • Aggregated metrics                                      │  │
│  │ • Business KPIs                                           │  │
│  │ • Ready for analytics                                     │  │
│  │ • Optimized for queries                                   │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LOADING LAYER                                  │
│                  (Databricks Notebook)                           │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ • Staging tables                                          │  │
│  │ • MERGE operations (upsert)                               │  │
│  │ • Transaction management                                  │  │
│  │ • Metadata logging                                        │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────────┘
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                   ORCHESTRATION                                  │
│                  Databricks Jobs                                 │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │ Orchestrator Notebook (00_orchestrator.py)               │  │
│  │ • Coordinates all phases                                  │  │
│  │ • Error handling                                          │  │
│  │ • Logging & monitoring                                    │  │
│  │ • Scheduled execution                                     │  │
│  └──────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

## Design Patterns

### 1. Medallion Architecture
Progressive data refinement through Bronze → Silver → Gold layers.

**Benefits:**
- Clear separation of concerns
- Incremental quality improvement
- Easy debugging and auditing
- Flexibility for different use cases

### 2. Type 2 Slowly Changing Dimensions (SCD)
Track historical changes in Silver layer.

**Fields:**
- `is_current`: Boolean indicating current record
- `valid_from`: Timestamp when record became active
- `valid_to`: Timestamp when record expired (NULL for current)

**Benefits:**
- Complete history preservation
- Point-in-time queries
- Change tracking

### 3. Staging + Merge Pattern
Load data through temporary staging tables.

**Process:**
1. Load to staging table
2. Execute MERGE statement
3. Clean up staging table

**Benefits:**
- Atomic operations
- Rollback capability
- Performance optimization

## Technology Choices

### Databricks
**Why?**
- Managed Spark environment
- Native notebook support
- Built-in job scheduling
- Excellent for large-scale processing

**Use Cases:**
- Data transformations
- Complex aggregations
- Distributed processing

### Snowflake
**Why?**
- Cloud-native data warehouse
- Zero-maintenance
- Auto-scaling
- Excellent query performance

**Use Cases:**
- Structured data storage
- Analytics queries
- BI tool integration

### DBFS (Databricks File System)
**Why?**
- Native to Databricks
- High-performance
- Distributed storage

**Use Cases:**
- Bronze layer storage
- Temporary files
- Intermediate results

## Data Flow

### Extraction Phase
1. Call CoinGecko API for multiple cryptocurrencies
2. Apply rate limiting (50 calls/minute)
3. Retry on failures (exponential backoff)
4. Save raw JSON to DBFS
5. Log extraction metrics

### Transformation Phase
1. Read JSON from DBFS into Spark DataFrame
2. Parse nested structures
3. Validate schema
4. Clean data (remove nulls, deduplicate)
5. Detect anomalies (Z-score method)
6. Create Silver DataFrame (cleaned)
7. Aggregate to Gold DataFrame (metrics)
8. Cache DataFrames for loading

### Loading Phase
1. Convert Spark DataFrames to Pandas
2. Connect to Snowflake
3. Load to staging tables (write_pandas)
4. Execute MERGE for Silver (Type 2 SCD)
5. Execute MERGE for Gold (simple upsert)
6. Log pipeline metadata
7. Disconnect

## Scalability Considerations

### Horizontal Scaling
- Spark: Add more worker nodes
- Snowflake: Auto-scales compute
- API: Parallel requests with rate limiting

### Vertical Scaling
- Databricks: Larger instance types
- Snowflake: Larger warehouse sizes

### Data Volume
- **Current**: Handles 10-100 cryptocurrencies
- **Scalable to**: 1000+ cryptocurrencies
- **Bottlenecks**: API rate limits, not processing

## Security

### Credentials Management
- **Development**: .env files (local only)
- **Production**: Databricks Secrets

### Network Security
- HTTPS for all API calls
- Encrypted Snowflake connections
- No credentials in code

### Data Security
- External browser auth (Snowflake)
- Immutable Bronze layer
- Audit logging in metadata tables

## Monitoring & Observability

### Logging
- Structured JSON logs
- Run ID tracking
- Event-based logging

### Metrics
- Records processed
- Duration per phase
- Error counts
- Data quality scores

### Alerting
- Job failure notifications
- Data quality violations
- Anomaly detection alerts

## Future Enhancements

### Short Term
- Real-time streaming ingestion
- Additional data sources
- Advanced visualizations

### Long Term
- Machine learning models
- Predictive analytics
- Multi-region deployment
