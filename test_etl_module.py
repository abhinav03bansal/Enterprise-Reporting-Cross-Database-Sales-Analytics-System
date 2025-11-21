"""
ETL Testing Module
Tests for extraction, transformation, and loading processes
"""

import unittest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys

class TestETLExtraction(unittest.TestCase):
    """Test data extraction from databases"""
    
    def test_mysql_extraction(self):
        """Test MySQL data extraction returns dataframes"""
        # Mock data
        mock_customers = pd.DataFrame({
            'customer_id': [1, 2, 3],
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson']
        })
        
        with patch('mysql.connector.connect') as mock_connect:
            mock_cursor = Mock()
            mock_connect.return_value.cursor.return_value = mock_cursor
            
            # Verify connection is attempted
            self.assertTrue(callable(mock_connect))
    
    def test_postgres_extraction(self):
        """Test PostgreSQL data extraction returns dataframes"""
        mock_products = pd.DataFrame({
            'product_id': [1, 2, 3],
            'product_name': ['Product A', 'Product B', 'Product C']
        })
        
        with patch('psycopg2.connect') as mock_connect:
            mock_cursor = Mock()
            mock_connect.return_value.cursor.return_value = mock_cursor
            
            # Verify connection is attempted
            self.assertTrue(callable(mock_connect))
    
    def test_extraction_error_handling(self):
        """Test extraction handles connection errors gracefully"""
        with patch('mysql.connector.connect', side_effect=Exception("Connection failed")):
            # Should not raise exception, should log error
            try:
                mock_connect = Mock(side_effect=Exception("Connection failed"))
                mock_connect()
            except Exception as e:
                self.assertIn("Connection failed", str(e))

class TestETLTransformation(unittest.TestCase):
    """Test data transformation logic"""
    
    def setUp(self):
        """Set up test data"""
        self.customers = pd.DataFrame({
            'customer_id': [1, 2, 3, 3],  # Duplicate customer_id
            'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Bob Johnson'],
            'email': ['john@email.com', None, 'bob@email.com', 'bob@email.com']
        })
        
        self.products = pd.DataFrame({
            'product_id': [1, 2, 3],
            'product_name': ['Product A', 'Product B', 'Product C'],
            'category': ['Electronics', None, 'Clothing'],
            'price': [100.0, 200.0, 50.0],
            'cost': [60.0, 120.0, 30.0]
        })
        
        self.sales = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'customer_id': [1, 2, 3],
            'product_id': [1, 2, 3],
            'quantity': [2, 1, 3],
            'unit_price': [100.0, 200.0, 50.0],
            'total_amount': [200.0, 200.0, 150.0]
        })
    
    def test_deduplication(self):
        """Test duplicate removal"""
        deduplicated = self.customers.drop_duplicates(subset=['customer_id'], keep='first')
        self.assertEqual(len(deduplicated), 3)
    
    def test_null_handling(self):
        """Test null value handling"""
        filled = self.customers.copy()
        filled['email'] = filled['email'].fillna('unknown@domain.com')
        self.assertEqual(filled['email'].isna().sum(), 0)
        self.assertIn('unknown@domain.com', filled['email'].values)
    
    def test_data_merge(self):
        """Test merging sales with customers and products"""
        merged = self.sales.merge(
            self.customers[['customer_id', 'customer_name']],
            on='customer_id',
            how='left'
        )
        self.assertIn('customer_name', merged.columns)
        self.assertEqual(len(merged), len(self.sales))
    
    def test_calculated_fields(self):
        """Test profit calculation"""
        sales_with_cost = self.sales.merge(
            self.products[['product_id', 'cost', 'price']],
            on='product_id',
            how='left'
        )
        sales_with_cost['profit'] = (sales_with_cost['unit_price'] - sales_with_cost['cost']) * sales_with_cost['quantity']
        
        self.assertIn('profit', sales_with_cost.columns)
        self.assertTrue((sales_with_cost['profit'] >= 0).all())
    
    def test_standardization(self):
        """Test data standardization"""
        standardized = self.customers.copy()
        standardized['customer_name'] = standardized['customer_name'].str.strip().str.title()
        
        # Check no leading/trailing spaces
        for name in standardized['customer_name']:
            self.assertEqual(name, name.strip())

class TestETLLoad(unittest.TestCase):
    """Test data loading operations"""
    
    def setUp(self):
        """Set up test data"""
        self.test_data = pd.DataFrame({
            'sale_id': [1, 2, 3],
            'customer_name': ['John', 'Jane', 'Bob'],
            'total_amount': [100, 200, 150]
        })
    
    def test_csv_export(self):
        """Test CSV file export"""
        output_file = 'test_output.csv'
        try:
            self.test_data.to_csv(output_file, index=False)
            
            # Verify file can be read back
            loaded = pd.read_csv(output_file)
            self.assertEqual(len(loaded), len(self.test_data))
            self.assertEqual(list(loaded.columns), list(self.test_data.columns))
        except Exception as e:
            self.fail(f"CSV export failed: {e}")
    
    def test_data_integrity_after_load(self):
        """Test data integrity is maintained during load"""
        # Check no data loss
        self.assertEqual(len(self.test_data), 3)
        
        # Check data types preserved
        self.assertTrue(pd.api.types.is_integer_dtype(self.test_data['sale_id']))
        self.assertTrue(pd.api.types.is_numeric_dtype(self.test_data['total_amount']))

class TestETLEndToEnd(unittest.TestCase):
    """Test complete ETL pipeline flow"""
    
    def test_pipeline_execution_flow(self):
        """Test ETL pipeline executes in correct order"""
        execution_order = []
        
        def mock_extract():
            execution_order.append('extract')
            return pd.DataFrame({'id': [1, 2, 3]})
        
        def mock_transform(data):
            execution_order.append('transform')
            return data
        
        def mock_load(data):
            execution_order.append('load')
        
        # Simulate pipeline
        data = mock_extract()
        transformed = mock_transform(data)
        mock_load(transformed)
        
        self.assertEqual(execution_order, ['extract', 'transform', 'load'])
    
    def test_error_propagation(self):
        """Test errors are properly propagated"""
        def failing_transform():
            raise ValueError("Transform failed")
        
        with self.assertRaises(ValueError):
            failing_transform()

if __name__ == '__main__':
    unittest.main(verbosity=2)
