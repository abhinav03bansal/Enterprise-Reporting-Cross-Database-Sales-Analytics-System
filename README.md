# Enterprise Reporting & Cross-Database Sales Analytics System

A comprehensive end-to-end analytics project featuring synthetic data generation, multi-database ETL pipeline, data validation, and Tableau-ready reporting capabilities.

## Project Overview

This system extracts sales data from MySQL and PostgreSQL databases, performs data quality validation, transforms and consolidates the data, and outputs Tableau-ready analytics datasets. It includes comprehensive testing, logging, performance optimization strategies, and root cause analysis tools.

## File Structure

```
project/
│
├── generate_data.py              # Synthetic data generation
├── insert_mysql.py               # MySQL database insertion
├── insert_postgres.py            # PostgreSQL database insertion
├── etl.py                        # Combined ETL pipeline
├── validation.py                 # Data quality validation
├── logging_utils.py              # Centralized logging
├── root_cause_analysis.py        # RCA and data profiling
│
├── tableau_custom_sql.sql        # Tableau custom SQL queries
├── tableau_calculated_fields.txt # Tableau calculated field definitions
├── tableau_lod_expressions.txt   # Tableau LOD expressions
│
├── performance_optimization.md   # Performance tuning guide
│
├── test_etl.py                   # ETL testing suite
├── test_sql_queries.py           # SQL query testing
├── test_validation.py            # Validation testing
│
├── README.md                     # This file
├── requirements.txt              # Python dependencies
│
├── data_customers.csv            # Generated customer data
├── data_products.csv             # Generated product data
└── data_sales.csv                # Generated sales data
```

## Prerequisites

- Python 3.8+
- MySQL Server
- PostgreSQL Server
- Tableau Desktop (for visualization)

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database connections:
   - MySQL: Default connection to `localhost:3306`
   - PostgreSQL: Default connection to `localhost:5432`
   - Update credentials in `insert_mysql.py` and `insert_postgres.py` as needed

## Usage Guide

### 1. Generate Synthetic Data

Generate 4,000 customers, 1,000 products, and 10,000 sales transactions:

```bash
python generate_data.py
```

**Output:**
- `data_customers.csv`
- `data_products.csv`
- `data_sales.csv`

### 2. Insert Data into Databases

**MySQL:**
```bash
python insert_mysql.py
```

**PostgreSQL:**
```bash
python insert_postgres.py
```

These scripts will:
- Create database tables if they don't exist
- Bulk insert data using executemany
- Log all operations
- Handle errors gracefully

### 3. Run ETL Pipeline

Execute the complete Extract-Transform-Load pipeline:

```bash
python etl.py
```

**ETL Process:**

**Extract:**
- Connects to MySQL and PostgreSQL
- Reads customers, products, and sales tables
- Logs extraction metrics

**Transform:**
- Combines data from both sources
- Performs deduplication
- Handles null values
- Standardizes data formats
- Merges datasets (sales + customers + products)
- Detects referential integrity issues
- Calculates derived metrics (profit, margin)

**Load:**
- Outputs consolidated dataset to `tableau_sales_data.csv`
- Ready for Tableau ingestion

### 4. Data Validation

Run comprehensive data quality checks:

```bash
python validation.py
```

**Validation Checks:**
- Null value detection
- Duplicate record identification
- Schema consistency verification
- Foreign key integrity validation
- Join quality assessment

### 5. Root Cause Analysis

Analyze data quality issues and mismatches:

```bash
python root_cause_analysis.py
```

**RCA Features:**
- Compares raw vs transformed data
- Identifies missing/extra records
- Detects foreign key violations
- Analyzes data distributions
- Outputs structured JSON results

## Tableau Integration

### Custom SQL Queries

Use queries from `tableau_custom_sql.sql` for:
- Multi-table joins with enriched sales data
- Monthly sales trends with growth calculations
- Customer ranking and segmentation
- Product performance analysis
- Running totals and moving averages
- Customer cohort analysis
- Executive dashboard summaries

### Calculated Fields

Import formulas from `tableau_calculated_fields.txt`:
- Revenue metrics (Total Revenue, Growth %, Revenue Per Customer)
- Profit metrics (Total Profit, Margin %, Profit Per Order)
- Order metrics (Total Orders, Completion Rate, AOV)
- Customer metrics (CLV, Retention Rate, Orders Per Customer)
- Time-based metrics (MoM Growth, YoY Growth, Running Total)

### LOD Expressions

Use expressions from `tableau_lod_expressions.txt`:
- **FIXED:** Customer-level, product-level, geographic aggregations
- **INCLUDE:** Adding dimensions for detailed analysis
- **EXCLUDE:** Removing dimensions for higher-level views

## Performance Optimization

Refer to `performance_optimization.md` for:
- Database indexing strategies
- Query optimization techniques
- Tableau extract performance tuning
- Incremental load implementation
- ETL performance best practices
- Resource allocation guidelines

**Quick Tips:**
- Create indexes on foreign keys and frequently queried columns
- Use extracts instead of live connections for large datasets
- Implement incremental refresh for daily updates
- Pre-aggregate data when full detail isn't needed

## Testing

### Run All Tests

```bash
# ETL tests
python test_etl.py

# SQL query tests
python test_sql_queries.py

# Validation tests
python test_validation.py
```

### Test Coverage

**ETL Tests:**
- Extraction from MySQL/PostgreSQL
- Data transformation logic
- CSV loading operations
- End-to-end pipeline flow

**SQL Query Tests:**
- Inner/left/outer joins
- Foreign key integrity
- Aggregation operations
- Filtering and sorting

**Validation Tests:**
- Null value detection
- Duplicate identification
- Schema matching
- Referential integrity
- Data quality metrics

## Architecture

```
┌─────────────────┐         ┌─────────────────┐
│     MySQL       │         │   PostgreSQL    │
│   Database      │         │    Database     │
└────────┬────────┘         └────────┬────────┘
         │                           │
         └───────────┬───────────────┘
                     │ EXTRACT
         ┌───────────▼────────────┐
         │   ETL Pipeline         │
         │  (Validation + RCA)    │
         └───────────┬────────────┘
                     │ TRANSFORM & LOAD
         ┌───────────▼────────────┐
         │  Tableau-Ready CSV     │
         └───────────┬────────────┘
                     │
         ┌───────────▼────────────┐
         │  Tableau Dashboard     │
         │  (Custom SQL + LODs)   │
         └────────────────────────┘
```

## Data Flow

1. **Data Generation:** Faker creates realistic synthetic data
2. **Database Loading:** Pandas + executemany bulk inserts
3. **Extraction:** Parallel extraction from MySQL and PostgreSQL
4. **Transformation:** Consolidation, deduplication, enrichment
5. **Validation:** Comprehensive quality checks and logging
6. **Loading:** Output to Tableau-ready format
7. **Visualization:** Tableau consumes enriched dataset

## Key Features

✅ Synthetic data generation with realistic distributions
✅ Multi-database support (MySQL + PostgreSQL)
✅ Comprehensive ETL pipeline with logging
✅ Data quality validation and monitoring
✅ Root cause analysis for data issues
✅ Tableau-optimized SQL and calculations
✅ LOD expressions for advanced analytics
✅ Performance optimization guidelines
✅ Complete test coverage
✅ Production-ready error handling

## Troubleshooting

**Database Connection Issues:**
- Verify MySQL/PostgreSQL services are running
- Check credentials in insert scripts
- Ensure databases exist or have CREATE permissions

**ETL Pipeline Errors:**
- Check logs for specific error messages
- Verify data files exist before running ETL
- Ensure sufficient disk space for output

**Performance Issues:**
- Implement indexes per optimization guide
- Use extract refreshes instead of live connections
- Consider data volume reduction strategies

## Best Practices

1. **Data Generation:** Run once to create baseline datasets
2. **Database Setup:** Create databases before insertion
3. **ETL Execution:** Run validation before and after ETL
4. **Tableau Usage:** Use extracts for datasets > 1M rows
5. **Testing:** Run tests after any code modifications
6. **Monitoring:** Check logs for warnings and errors
7. **Optimization:** Apply indexing before production use

## Maintenance

**Daily:**
- Monitor ETL execution logs
- Check validation results

**Weekly:**
- Review performance metrics
- Update incremental loads

**Monthly:**
- Analyze and optimize slow queries
- Archive old data
- Update table statistics

## License

This project is provided as-is for analytics and educational purposes.

## Contact

For questions or issues, please refer to the logging output and validation reports for detailed diagnostics.
