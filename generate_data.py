"""
Synthetic Data Generation for Enterprise Analytics System
Generates customers, products, and sales data using Faker
"""

import pandas as pd
from faker import Faker
import random
from datetime import datetime, timedelta

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_customers(n=4000):
    """Generate customer data"""
    customers = []
    for i in range(1, n + 1):
        customers.append({
            'customer_id': i,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.address().replace('\n', ', '),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.zipcode(),
            'country': 'USA',
            'registration_date': fake.date_between(start_date='-5y', end_date='today')
        })
    return pd.DataFrame(customers)

def generate_products(n=1000):
    """Generate product data"""
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food']
    products = []
    for i in range(1, n + 1):
        products.append({
            'product_id': i,
            'product_name': fake.catch_phrase(),
            'category': random.choice(categories),
            'price': round(random.uniform(10, 1000), 2),
            'cost': round(random.uniform(5, 500), 2),
            'supplier': fake.company(),
            'stock_quantity': random.randint(0, 500)
        })
    return pd.DataFrame(products)

def generate_sales(n=10000, customers_df=None, products_df=None):
    """Generate sales transactions"""
    sales = []
    start_date = datetime.now() - timedelta(days=730)
    
    for i in range(1, n + 1):
        customer_id = random.choice(customers_df['customer_id'].tolist())
        product_id = random.choice(products_df['product_id'].tolist())
        quantity = random.randint(1, 10)
        price = products_df[products_df['product_id'] == product_id]['price'].values[0]
        
        sales.append({
            'sale_id': i,
            'customer_id': customer_id,
            'product_id': product_id,
            'sale_date': fake.date_time_between(start_date=start_date, end_date='now'),
            'quantity': quantity,
            'unit_price': price,
            'total_amount': round(quantity * price, 2),
            'payment_method': random.choice(['Credit Card', 'Debit Card', 'PayPal', 'Cash']),
            'status': random.choice(['Completed', 'Pending', 'Cancelled'])
        })
    return pd.DataFrame(sales)

if __name__ == '__main__':
    print("Generating synthetic data...")
    
    # Generate datasets
    customers = generate_customers(4000)
    products = generate_products(1000)
    sales = generate_sales(10000, customers, products)
    
    # Save to CSV
    customers.to_csv('data_customers.csv', index=False)
    products.to_csv('data_products.csv', index=False)
    sales.to_csv('data_sales.csv', index=False)
    
    print(f"✓ Generated {len(customers)} customers -> data_customers.csv")
    print(f"✓ Generated {len(products)} products -> data_products.csv")
    print(f"✓ Generated {len(sales)} sales -> data_sales.csv")
    print("Data generation complete!")
