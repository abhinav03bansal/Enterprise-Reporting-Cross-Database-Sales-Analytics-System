"""
SQL Query Testing Module
Tests SQL joins, integrity checks, and query correctness
"""

import unittest
import pandas as pd
from unittest.mock import Mock, patch

class TestSQLJoins(unittest.TestCase):
    """Test SQL join operations"""
    
    def setUp(self):
        """Set up test dataframes simulating database tables"""
        self.customers = pd.DataFrame({
            'customer_id': [1, 2, 3, 4],
            'customer_name': ['Alice', 'Bob', 'Charlie', 'David']
        })
        
        self.products = pd.DataFrame({
            'product_id': [101, 102, 103],
            'product_name': ['Widget', 'Gadget', 'Tool'],
            'price': [10.0, 20.0, 15.0]
        })
        
        self.sales = pd.DataFrame({
            'sale_id': [1, 2, 3, 4, 5],
            'customer_id': [1, 2, 1, 3, 5],  # customer_id 5 doesn't exist
            'product_id': [101, 102, 103, 101, 102],
            'quantity': [2, 1, 3, 1, 2]
        })
    
    def test_inner_join_customers_sales(self):
        """Test INNER JOIN between customers and sales"""
        result = self.sales.merge(
            self.customers,
            on='customer_id',
            how='inner'
        )
        
        # Should only include sales with valid customers
        self.assertEqual(len(result), 4)
        self.assertNotIn(5, result['customer_id'].values)
    
    def test_left_join_sales_customers(self):
        """Test LEFT JOIN preserves all sales records"""
        result = self.sales.merge(
            self.customers,
            on='customer_id',
            how='left'
        )
        
        # Should include all sales, even orphaned ones
        self.assertEqual(len(result), 5)
        self.assertTrue(result['customer_name'].isna().any())
    
    def test_three_table_join(self):
        """Test joining sales, customers, and products"""
        result = self.sales.merge(
            self.customers,
            on='customer_id',
            how='inner'
        ).merge(
            self.products,
            on='product_id',
            how='inner'
        )
        
        # Should have columns from all three tables
        self.assertIn('customer_name', result.columns)
        self.assertIn('product_name', result.columns)
        self.assertIn('sale_id', result.columns)
        
        # Should only include valid customer-product combinations
        self.assertEqual(len(result), 4)

class TestDataIntegrity(unittest.TestCase):
    """Test data integrity and referential constraints"""
    
    def setUp(self):
        """Set up test data"""
        self.customers = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'customer_name': ['Alice', 'Bob', 'Charlie']
        })
        
        self.sales = pd.DataFrame({
            'sale_id': [1, 2, 3, 4],
            'customer_id': [1, 2, 1, 99],  # 99 is invalid
            'total_amount': [100, 200, 150, 300]
        })
    
    def test_foreign_key_integrity(self):
        """Test foreign key constraint checking"""
        valid_customer_ids = set(self.customers['customer_id'])
        sales_customer_ids = set(self.sales['customer_id'])
        
        # Find orphaned records
        orphaned = sales_customer_ids - valid_customer_ids
        
        self.assertGreater(len(orphaned), 0)
        self.assertIn(99, orphaned)
    
    def test_referential_integrity_check(self):
        """Test all foreign keys exist in parent table"""
        merged = self.sales.merge(
            self.customers,
            on='customer_id',
            how='left',
            indicator=True
        )
        
        # Check for unmatched records
        unmatched = merged[merged['_merge'] == 'left_only']
        self.assertEqual(len(unmatched), 1)
        self.assertEqual(unmatched['customer_id'].values[0], 99)
    
    def test_primary_key_uniqueness(self):
        """Test primary key uniqueness"""
        # Check for duplicates in primary key
        duplicates = self.customers[self.customers.duplicated(subset=['customer_id'], keep=False)]
        self.assertEqual(len(duplicates), 0)
        
        # Check sales primary key
        sales_duplicates = self.sales[self.sales.duplicated(subset=['sale_id'], keep=False)]
        self.assertEqual(len(sales_duplicates), 0)

class TestAggregationQueries(unittest.TestCase):
    """Test SQL aggregation queries"""
    
    def setUp(self):
        """Set up test data"""
        self.sales = pd.DataFrame({
            'sale_id': [1, 2, 3, 4, 5],
            'customer_id': [1, 1, 2, 2, 3],
            'product_id': [101, 102, 101, 103, 102],
            'quantity': [2, 1, 3, 1, 2],
            'total_amount': [100, 50, 150, 75, 100]
        })
    
    def test_group_by_customer(self):
        """Test GROUP BY customer aggregation"""
        result = self.sales.groupby('customer_id').agg({
            'sale_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        
        result.columns = ['customer_id', 'order_count', 'total_spent']
        
        # Check customer 1 has 2 orders
        customer_1 = result[result['customer_id'] == 1]
        self.assertEqual(customer_1['order_count'].values[0], 2)
        self.assertEqual(customer_1['total_spent'].values[0], 150)
    
    def test_sum_aggregation(self):
        """Test SUM aggregation"""
        total_revenue = self.sales['total_amount'].sum()
        self.assertEqual(total_revenue, 475)
    
    def test_count_aggregation(self):
        """Test COUNT aggregation"""
        total_orders = len(self.sales)
        self.assertEqual(total_orders, 5)
        
        unique_customers = self.sales['customer_id'].nunique()
        self.assertEqual(unique_customers, 3)
    
    def test_average_calculation(self):
        """Test AVG calculation"""
        avg_order_value = self.sales['total_amount'].mean()
        self.assertEqual(avg_order_value, 95.0)

class TestFilteringQueries(unittest.TestCase):
    """Test WHERE clause filtering"""
    
    def setUp(self):
        """Set up test data"""
        self.sales = pd.DataFrame({
            'sale_id': [1, 2, 3, 4, 5],
            'customer_id': [1, 2, 3, 4, 5],
            'total_amount': [100, 200, 50, 300, 150],
            'status': ['Completed', 'Completed', 'Pending', 'Completed', 'Cancelled']
        })
    
    def test_single_condition_filter(self):
        """Test single WHERE condition"""
        completed = self.sales[self.sales['status'] == 'Completed']
        self.assertEqual(len(completed), 3)
    
    def test_multiple_condition_filter(self):
        """Test multiple WHERE conditions with AND"""
        result = self.sales[
            (self.sales['status'] == 'Completed') & 
            (self.sales['total_amount'] >= 200)
        ]
        self.assertEqual(len(result), 2)
    
    def test_or_condition_filter(self):
        """Test OR condition"""
        result = self.sales[
            (self.sales['status'] == 'Pending') | 
            (self.sales['status'] == 'Cancelled')
        ]
        self.assertEqual(len(result), 2)
    
    def test_in_operator(self):
        """Test IN operator"""
        valid_statuses = ['Completed', 'Pending']
        result = self.sales[self.sales['status'].isin(valid_statuses)]
        self.assertEqual(len(result), 4)

class TestSortingQueries(unittest.TestCase):
    """Test ORDER BY sorting"""
    
    def setUp(self):
        """Set up test data"""
        self.products = pd.DataFrame({
            'product_id': [3, 1, 4, 2],
            'product_name': ['C', 'A', 'D', 'B'],
            'price': [30, 10, 40, 20]
        })
    
    def test_ascending_sort(self):
        """Test ORDER BY ASC"""
        sorted_df = self.products.sort_values('price', ascending=True)
        self.assertEqual(sorted_df['price'].values[0], 10)
        self.assertEqual(sorted_df['price'].values[-1], 40)
    
    def test_descending_sort(self):
        """Test ORDER BY DESC"""
        sorted_df = self.products.sort_values('price', ascending=False)
        self.assertEqual(sorted_df['price'].values[0], 40)
        self.assertEqual(sorted_df['price'].values[-1], 10)
    
    def test_multiple_column_sort(self):
        """Test ORDER BY multiple columns"""
        df = pd.DataFrame({
            'category': ['A', 'B', 'A', 'B'],
            'price': [100, 200, 50, 150]
        })
        sorted_df = df.sort_values(['category', 'price'], ascending=[True, False])
        
        # Check category A comes first and is sorted by price DESC
        category_a = sorted_df[sorted_df['category'] == 'A']
        self.assertEqual(category_a['price'].values[0], 100)

if __name__ == '__main__':
    unittest.main(verbosity=2)
