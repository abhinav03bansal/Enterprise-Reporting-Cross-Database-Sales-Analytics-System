"""
Combined ETL Pipeline: Extract, Transform, Load
Consolidates data from MySQL and PostgreSQL into a Tableau-ready dataset
"""

import pandas as pd
import mysql.connector
import psycopg2
from logging_utils import log_action, log_error, log_duration
import time

# ==================== EXTRACT ====================

def extract_from_mysql(host='localhost', database='analytics_db', user='root', password='password'):
    """Extract data from MySQL database"""
    log_action("Starting MySQL extraction")
    start_time = time.time()
    
    try:
        connection = mysql.connector.connect(
            host=host, database=database, user=user, password=password
        )
        
        customers_mysql = pd.read_sql("SELECT * FROM customers", connection)
        products_mysql = pd.read_sql("SELECT * FROM products", connection)
        sales_mysql = pd.read_sql("SELECT * FROM sales", connection)
        
        connection.close()
        
        log_duration("MySQL extraction", time.time() - start_time)
        log_action(f"Extracted from MySQL: {len(customers_mysql)} customers, {len(products_mysql)} products, {len(sales_mysql)} sales")
        
        return customers_mysql, products_mysql, sales_mysql
    except Exception as e:
        log_error(f"MySQL extraction failed: {e}")
        return None, None, None

def extract_from_postgres(host='localhost', database='analytics_db', user='postgres', password='password', port='5432'):
    """Extract data from PostgreSQL database"""
    log_action("Starting PostgreSQL extraction")
    start_time = time.time()
    
    try:
        connection = psycopg2.connect(
            host=host, database=database, user=user, password=password, port=port
        )
        
        customers_pg = pd.read_sql("SELECT * FROM customers", connection)
        products_pg = pd.read_sql("SELECT * FROM products", connection)
        sales_pg = pd.read_sql("SELECT * FROM sales", connection)
        
        connection.close()
        
        log_duration("PostgreSQL extraction", time.time() - start_time)
        log_action(f"Extracted from PostgreSQL: {len(customers_pg)} customers, {len(products_pg)} products, {len(sales_pg)} sales")
        
        return customers_pg, products_pg, sales_pg
    except Exception as e:
        log_error(f"PostgreSQL extraction failed: {e}")
        return None, None, None

# ==================== TRANSFORM ====================

def transform_data(customers_mysql, products_mysql, sales_mysql, 
                   customers_pg, products_pg, sales_pg):
    """Transform and consolidate data from both sources"""
    log_action("Starting data transformation")
    start_time = time.time()
    
    # Combine data from both sources
    customers_combined = pd.concat([customers_mysql, customers_pg], ignore_index=True)
    products_combined = pd.concat([products_mysql, products_pg], ignore_index=True)
    sales_combined = pd.concat([sales_mysql, sales_pg], ignore_index=True)
    
    log_action(f"Combined datasets: {len(customers_combined)} customers, {len(products_combined)} products, {len(sales_combined)} sales")
    
    # Deduplication
    customers_combined = customers_combined.drop_duplicates(subset=['customer_id'], keep='first')
    products_combined = products_combined.drop_duplicates(subset=['product_id'], keep='first')
    sales_combined = sales_combined.drop_duplicates(subset=['sale_id'], keep='first')
    
    log_action(f"After deduplication: {len(customers_combined)} customers, {len(products_combined)} products, {len(sales_combined)} sales")
    
    # Null handling
    customers_combined['email'] = customers_combined['email'].fillna('unknown@domain.com')
    customers_combined['phone'] = customers_combined['phone'].fillna('000-000-0000')
    products_combined['category'] = products_combined['category'].fillna('Uncategorized')
    
    # Standardization
    customers_combined['customer_name'] = customers_combined['customer_name'].str.strip().str.title()
    customers_combined['email'] = customers_combined['email'].str.lower()
    products_combined['category'] = products_combined['category'].str.strip().str.title()
    
    # Merge datasets
    sales_enriched = sales_combined.merge(
        customers_combined[['customer_id', 'customer_name', 'email', 'city', 'state', 'country']],
        on='customer_id',
        how='left'
    )
    
    sales_enriched = sales_enriched.merge(
        products_combined[['product_id', 'product_name', 'category', 'price', 'cost']],
        on='product_id',
        how='left'
    )
    
    # Detect mismatches
    missing_customers = sales_enriched['customer_name'].isna().sum()
    missing_products = sales_enriched['product_name'].isna().sum()
    
    if missing_customers > 0:
        log_error(f"Found {missing_customers} sales with missing customer references")
    if missing_products > 0:
        log_error(f"Found {missing_products} sales with missing product references")
    
    # Integrity checks
    sales_enriched = sales_enriched.dropna(subset=['customer_name', 'product_name'])
    
    # Calculate additional metrics
    sales_enriched['profit'] = (sales_enriched['unit_price'] - sales_enriched['cost']) * sales_enriched['quantity']
    sales_enriched['margin_percent'] = ((sales_enriched['unit_price'] - sales_enriched['cost']) / sales_enriched['unit_price'] * 100).round(2)
    
    log_duration("Data transformation", time.time() - start_time)
    log_action(f"Transformation complete: {len(sales_enriched)} final records")
    
    return sales_enriched, customers_combined, products_combined

# ==================== LOAD ====================

def load_data(sales_enriched, output_file='tableau_sales_data.csv'):
    """Load transformed data to CSV for Tableau"""
    log_action("Starting data load")
    start_time = time.time()
    
    try:
        sales_enriched.to_csv(output_file, index=False)
        log_action(f"✓ Data loaded successfully to {output_file}")
        log_duration("Data load", time.time() - start_time)
    except Exception as e:
        log_error(f"Failed to save output file: {e}")

# ==================== MAIN ETL FLOW ====================

def run_etl():
    """Execute the complete ETL pipeline"""
    log_action("=" * 60)
    log_action("STARTING ETL PIPELINE")
    log_action("=" * 60)
    
    # Extract
    customers_mysql, products_mysql, sales_mysql = extract_from_mysql()
    customers_pg, products_pg, sales_pg = extract_from_postgres()
    
    if customers_mysql is None or customers_pg is None:
        log_error("Extraction failed. Aborting ETL.")
        return
    
    # Transform
    sales_enriched, customers_final, products_final = transform_data(
        customers_mysql, products_mysql, sales_mysql,
        customers_pg, products_pg, sales_pg
    )
    
    # Load
    load_data(sales_enriched)
    
    log_action("=" * 60)
    log_action("ETL PIPELINE COMPLETED SUCCESSFULLY")
    log_action("=" * 60)

if __name__ == '__main__':
    run_etl()
