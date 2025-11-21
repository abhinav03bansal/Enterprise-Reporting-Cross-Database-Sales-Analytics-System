# Performance Optimization Guide

## Database Indexing Strategy

### Primary Indexes
Create indexes on frequently queried columns to improve query performance:

```sql
-- Customer table indexes
CREATE INDEX idx_customer_id ON customers(customer_id);
CREATE INDEX idx_customer_email ON customers(email);
CREATE INDEX idx_customer_city_state ON customers(city, state);

-- Product table indexes
CREATE INDEX idx_product_id ON products(product_id);
CREATE INDEX idx_product_category ON products(category);

-- Sales table indexes
CREATE INDEX idx_sale_id ON sales(sale_id);
CREATE INDEX idx_sale_customer_id ON sales(customer_id);
CREATE INDEX idx_sale_product_id ON sales(product_id);
CREATE INDEX idx_sale_date ON sales(sale_date);
CREATE INDEX idx_sale_status ON sales(status);
```

### Composite Indexes
For queries that filter on multiple columns:

```sql
-- Common join patterns
CREATE INDEX idx_sales_customer_product ON sales(customer_id, product_id);
CREATE INDEX idx_sales_date_status ON sales(sale_date, status);

-- Time-based analysis
CREATE INDEX idx_sales_date_customer ON sales(sale_date, customer_id);
CREATE INDEX idx_sales_date_product ON sales(sale_date, product_id);
```

### When to Use Indexes
- Columns used in WHERE clauses
- Columns used in JOIN conditions
- Columns used in ORDER BY clauses
- Foreign key columns
- Columns with high cardinality

### When NOT to Use Indexes
- Small tables (< 1000 rows)
- Columns with low cardinality
- Columns that are frequently updated
- Tables with heavy write operations

## Query Optimization

### Use Explicit Column Selection
Instead of `SELECT *`, specify only needed columns:

```sql
-- Bad
SELECT * FROM sales;

-- Good
SELECT sale_id, customer_id, total_amount, sale_date FROM sales;
```

### Optimize JOIN Operations
Order joins from smallest to largest tables:

```sql
-- Join smaller tables first
SELECT s.sale_id, c.customer_name, p.product_name
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
INNER JOIN customers c ON s.customer_id = c.customer_id;
```

### Use WHERE Filters Early
Apply filters before joins when possible:

```sql
-- Filter before joining
SELECT s.sale_id, c.customer_name
FROM (SELECT * FROM sales WHERE status = 'Completed') s
INNER JOIN customers c ON s.customer_id = c.customer_id;
```

### Avoid Subqueries in SELECT
Move subqueries to JOINs or CTEs:

```sql
-- Bad
SELECT customer_id, 
       (SELECT SUM(total_amount) FROM sales WHERE customer_id = c.customer_id) as total
FROM customers c;

-- Good
SELECT c.customer_id, SUM(s.total_amount) as total
FROM customers c
LEFT JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id;
```

### Use EXISTS Instead of IN for Subqueries
```sql
-- Less efficient
SELECT * FROM customers WHERE customer_id IN (SELECT customer_id FROM sales);

-- More efficient
SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM sales s WHERE s.customer_id = c.customer_id);
```

### Partition Large Queries
Break down queries by date ranges:

```sql
-- Process in chunks
SELECT * FROM sales WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31';
SELECT * FROM sales WHERE sale_date BETWEEN '2024-02-01' AND '2024-02-28';
```

## Tableau Extract Performance

### Extract Optimization
- Use extracts instead of live connections for large datasets
- Schedule extract refreshes during off-peak hours
- Use incremental extract refreshes when possible

### Incremental Refresh Setup
```sql
-- Add a last_modified column for incremental updates
ALTER TABLE sales ADD COLUMN last_modified TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;
```

### Data Source Filters
Apply filters at the data source level to reduce data volume:
- Filter by date range (last 2 years)
- Filter by status (exclude 'Cancelled')
- Filter by relevant geographic regions

### Aggregation at Source
Pre-aggregate data when full detail isn't needed:

```sql
-- Monthly aggregated extract
SELECT 
    DATE_FORMAT(sale_date, '%Y-%m') as month,
    customer_id,
    product_id,
    SUM(total_amount) as monthly_revenue,
    COUNT(sale_id) as order_count
FROM sales
GROUP BY DATE_FORMAT(sale_date, '%Y-%m'), customer_id, product_id;
```

### Optimize Calculated Fields
- Move complex calculations to the database
- Use FIXED LOD expressions sparingly
- Pre-calculate derived fields in ETL

### Extract Size Recommendations
- Keep extracts under 1GB for optimal performance
- Consider data source filters to reduce size
- Archive historical data older than 2-3 years

## Incremental Load Strategy

### Timestamp-Based Incremental Load
```python
# Track last successful load
last_load_time = get_last_load_timestamp()

# Extract only new/modified records
query = f"""
SELECT * FROM sales 
WHERE last_modified > '{last_load_time}'
"""
```

### Date-Based Incremental Load
```python
# Load only recent data
query = """
SELECT * FROM sales 
WHERE sale_date >= CURRENT_DATE - INTERVAL 7 DAY
"""
```

### Change Data Capture (CDC)
Track changes using a staging table:

```sql
-- Create staging table
CREATE TABLE sales_staging LIKE sales;

-- Insert new records
INSERT INTO sales_staging SELECT * FROM sales WHERE last_modified > :last_load_time;

-- Merge into target
MERGE INTO sales_target t
USING sales_staging s ON t.sale_id = s.sale_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

## ETL Performance Best Practices

### Batch Processing
Process data in chunks to avoid memory issues:

```python
chunk_size = 10000
for chunk in pd.read_sql(query, connection, chunksize=chunk_size):
    process_chunk(chunk)
```

### Parallel Processing
Use multiprocessing for independent operations:

```python
from multiprocessing import Pool

def process_table(table_name):
    # Process logic
    pass

with Pool(4) as pool:
    pool.map(process_table, ['customers', 'products', 'sales'])
```

### Connection Pooling
Reuse database connections:

```python
from sqlalchemy import create_engine
engine = create_engine('mysql://user:pass@host/db', pool_size=10)
```

### Optimize Pandas Operations
- Use vectorized operations instead of loops
- Use `pd.read_sql` with `chunksize` for large datasets
- Use `dtype` parameter to reduce memory usage
- Use `to_csv` with `chunksize` for large exports

## Monitoring and Maintenance

### Query Performance Monitoring
```sql
-- MySQL: Enable slow query log
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;

-- PostgreSQL: Check query performance
SELECT query, calls, total_time, mean_time 
FROM pg_stat_statements 
ORDER BY total_time DESC LIMIT 10;
```

### Index Maintenance
```sql
-- MySQL: Analyze tables
ANALYZE TABLE customers, products, sales;

-- PostgreSQL: Vacuum and analyze
VACUUM ANALYZE customers;
VACUUM ANALYZE products;
VACUUM ANALYZE sales;
```

### Regular Cleanup
- Archive old data periodically
- Remove unused indexes
- Update statistics regularly
- Monitor disk space usage

## Tableau Workbook Optimization

### Dashboard Best Practices
- Limit number of worksheets per dashboard (max 10-12)
- Avoid dashboard actions that trigger multiple queries
- Use dashboard filters instead of individual worksheet filters
- Minimize use of sets and groups

### Visualization Performance
- Use simpler chart types when possible
- Limit number of marks displayed (< 10,000)
- Avoid transparent marks
- Use extracts with aggregated data for scatter plots

### Calculation Efficiency
- Replace table calculations with database calculations
- Use ATTR() instead of MIN/MAX for single-value dimensions
- Avoid nested LOD expressions
- Pre-calculate complex metrics in ETL

## Resource Allocation

### Database Server
- Allocate sufficient RAM (64GB+ for production)
- Use SSD storage for database files
- Configure connection pool size appropriately
- Monitor CPU and memory usage

### Tableau Server
- Allocate 8GB+ RAM per core
- Use dedicated extract refresh schedule
- Configure backgrounder processes (2-4 per core)
- Enable extract encryption for security
