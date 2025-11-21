"""
MySQL Database Insertion Script
Connects to MySQL and bulk inserts customer, product, and sales data
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_connection(host='localhost', database='analytics_db', user='root', password='password'):
    """Create MySQL database connection"""
    try:
        connection = mysql.connector.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        if connection.is_connected():
            logging.info(f"Connected to MySQL database: {database}")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def create_tables(connection):
    """Create tables if they don't exist"""
    cursor = connection.cursor()
    
    tables = {
        'customers': """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY,
                customer_name VARCHAR(255),
                email VARCHAR(255),
                phone VARCHAR(50),
                address TEXT,
                city VARCHAR(100),
                state VARCHAR(50),
                zip_code VARCHAR(20),
                country VARCHAR(100),
                registration_date DATE
            )
        """,
        'products': """
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY,
                product_name VARCHAR(255),
                category VARCHAR(100),
                price DECIMAL(10, 2),
                cost DECIMAL(10, 2),
                supplier VARCHAR(255),
                stock_quantity INT
            )
        """,
        'sales': """
            CREATE TABLE IF NOT EXISTS sales (
                sale_id INT PRIMARY KEY,
                customer_id INT,
                product_id INT,
                sale_date DATETIME,
                quantity INT,
                unit_price DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                payment_method VARCHAR(50),
                status VARCHAR(50),
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """
    }
    
    for table_name, create_stmt in tables.items():
        try:
            cursor.execute(create_stmt)
            logging.info(f"Table '{table_name}' created or already exists")
        except Error as e:
            logging.error(f"Error creating table {table_name}: {e}")
    
    cursor.close()

def bulk_insert(connection, table_name, df):
    """Bulk insert data using executemany"""
    cursor = connection.cursor()
    
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        data_tuples = [tuple(row) for row in df.values]
        cursor.executemany(insert_query, data_tuples)
        connection.commit()
        logging.info(f"✓ Inserted {cursor.rowcount} rows into {table_name}")
    except Error as e:
        logging.error(f"Error inserting into {table_name}: {e}")
        connection.rollback()
    finally:
        cursor.close()

if __name__ == '__main__':
    # Load CSV files
    logging.info("Loading CSV files...")
    customers_df = pd.read_csv('data_customers.csv')
    products_df = pd.read_csv('data_products.csv')
    sales_df = pd.read_csv('data_sales.csv')
    
    # Connect to MySQL
    conn = create_connection()
    
    if conn:
        try:
            # Create tables
            create_tables(conn)
            
            # Insert data in order (customers, products, then sales for FK constraints)
            bulk_insert(conn, 'customers', customers_df)
            bulk_insert(conn, 'products', products_df)
            bulk_insert(conn, 'sales', sales_df)
            
            logging.info("All data successfully inserted into MySQL!")
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
        finally:
            conn.close()
            logging.info("MySQL connection closed")
