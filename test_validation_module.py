"""
Validation Testing Module
Tests data quality validation functions
"""

import unittest
import pandas as pd
from io import StringIO

class TestNullValueChecks(unittest.TestCase):
    """Test null value detection"""
    
    def test_no_null_values(self):
        """Test dataset with no null values"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [100, 200, 300]
        })
        
        null_counts = df.isnull().sum()
        self.assertEqual(null_counts.sum(), 0)
    
    def test_null_values_present(self):
        """Test dataset with null values"""
        df = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', None, 'Charlie'],
            'email': ['alice@email.com', 'bob@email.com', None]
        })
        
        null_counts = df.isnull().sum()
        self.assertEqual(null_counts['name'], 1)
        self.assertEqual(null_counts['email'], 1)
        self.assertGreater(null_counts.sum(), 0)
    
    def test_null_percentage_calculation(self):
        """Test calculating null percentage"""
        df = pd.DataFrame({
            'col1': [1, None, 3, None, 5],
            'col2': [10, 20, 30, 40, 50]
        })
        
        null_pct = (df['col1'].isnull().sum() / len(df)) * 100
        self.assertEqual(null_pct, 40.0)

class TestDuplicateChecks(unittest.TestCase):
    """Test duplicate record detection"""
    
    def test_no_duplicates(self):
        """Test dataset with unique records"""
        df = pd.DataFrame({
            'customer_id': [1, 2, 3, 4],
            'name': ['Alice', 'Bob', 'Charlie', 'David']
        })
        
        duplicates = df[df.duplicated(subset=['customer_id'], keep=False)]
        self.assertEqual(len(duplicates), 0)
    
    def test_duplicates_present(self):
        """Test dataset with duplicate records"""
        df = pd.DataFrame({
            'customer_id': [1, 2, 3, 2, 4],
            'name': ['Alice', 'Bob', 'Charlie', 'Bob', 'David']
        })
        
        duplicates = df[df.duplicated(subset=['customer_id'], keep=False)]
        self.assertEqual(len(duplicates), 2)  # Both occurrences of customer_id 2
    
    def test_duplicate_removal(self):
        """Test removing duplicates"""
        df = pd.DataFrame({
            'id': [1, 2, 3, 2, 4],
            'value': ['A', 'B', 'C', 'B', 'D']
        })
        
        deduplicated = df.drop_duplicates(subset=['id'], keep='first')
        self.assertEqual(len(deduplicated), 4)
        self.assertEqual(list(deduplicated['id']), [1, 2, 3, 4])
    
    def test_duplicate_count(self):
        """Test counting duplicate records"""
        df = pd.DataFrame({
            'key': [1, 1, 2, 2, 2, 3]
        })
        
        duplicate_count = df.duplicated(subset=['key']).sum()
        self.assertEqual(duplicate_count, 3)  # 1 duplicate of key 1, 2 duplicates of key 2

class TestSchemaValidation(unittest.TestCase):
    """Test schema matching and validation"""
    
    def test_schema_match(self):
        """Test schemas match between two dataframes"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice'], 'value': [100]})
        df2 = pd.DataFrame({'id': [2], 'name': ['Bob'], 'value': [200]})
        
        self.assertEqual(set(df1.columns), set(df2.columns))
    
    def test_schema_mismatch(self):
        """Test schemas don't match"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'id': [2], 'email': ['bob@email.com']})
        
        self.assertNotEqual(set(df1.columns), set(df2.columns))
    
    def test_extra_columns(self):
        """Test identifying extra columns"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice']})
        df2 = pd.DataFrame({'id': [2], 'name': ['Bob'], 'email': ['bob@email.com']})
        
        extra_in_df2 = set(df2.columns) - set(df1.columns)
        self.assertEqual(extra_in_df2, {'email'})
    
    def test_missing_columns(self):
        """Test identifying missing columns"""
        df1 = pd.DataFrame({'id': [1], 'name': ['Alice'], 'email': ['alice@email.com']})
        df2 = pd.DataFrame({'id': [2], 'name': ['Bob']})
        
        missing_in_df2 = set(df1.columns) - set(df2.columns)
        self.assertEqual(missing_in_df2, {'email'})

class TestReferentialIntegrity(unittest.TestCase):
    """Test foreign key and referential integrity"""
    
    def test_valid_foreign_keys(self):
        """Test all foreign keys exist in parent table"""
        parent = pd.DataFrame({'parent_id': [1, 2, 3, 4]})
        child = pd.DataFrame({'child_id': [1, 2, 3], 'parent_id': [1, 2, 3]})
        
        parent_keys = set(parent['parent_id'])
        child_keys = set(child['parent_id'])
        
        orphaned = child_keys - parent_keys
        self.assertEqual(len(orphaned), 0)
    
    def test_orphaned_records(self):
        """Test detecting orphaned child records"""
        parent = pd.DataFrame({'parent_id': [1, 2, 3]})
        child = pd.DataFrame({'child_id': [1, 2, 3, 4], 'parent_id': [1, 2, 5, 6]})
        
        parent_keys = set(parent['parent_id'])
        child_keys = set(child['parent_id'])
        
        orphaned = child_keys - parent_keys
        self.assertEqual(orphaned, {5, 6})
    
    def test_customer_sales_integrity(self):
        """Test customer-sales referential integrity"""
        customers = pd.DataFrame({'customer_id': [1, 2, 3]})
        sales = pd.DataFrame({
            'sale_id': [1, 2, 3, 4],
            'customer_id': [1, 2, 3, 99]  # 99 is invalid
        })
        
        valid_customers = set(customers['customer_id'])
        sales_customers = set(sales['customer_id'])
        
        invalid = sales_customers - valid_customers
        self.assertEqual(invalid, {99})
        self.assertEqual(len(invalid), 1)

class TestJoinIntegrity(unittest.TestCase):
    """Test join operations maintain data integrity"""
    
    def test_inner_join_no_data_loss(self):
        """Test inner join doesn't lose expected records"""
        df1 = pd.DataFrame({'id': [1, 2, 3], 'val1': ['A', 'B', 'C']})
        df2 = pd.DataFrame({'id': [1, 2, 3], 'val2': ['X', 'Y', 'Z']})
        
        merged = df1.merge(df2, on='id', how='inner')
        self.assertEqual(len(merged), 3)
    
    def test_left_join_preserves_left_records(self):
        """Test left join preserves all left table records"""
        df1 = pd.DataFrame({'id': [1, 2, 3, 4], 'val1': ['A', 'B', 'C', 'D']})
        df2 = pd.DataFrame({'id': [1, 2], 'val2': ['X', 'Y']})
        
        merged = df1.merge(df2, on='id', how='left')
        self.assertEqual(len(merged), 4)
        self.assertEqual(len(merged), len(df1))
    
    def test_join_creates_duplicates(self):
        """Test detecting when join creates duplicate records"""
        df1 = pd.DataFrame({'id': [1, 2], 'val1': ['A', 'B']})
        df2 = pd.DataFrame({'id': [1, 1, 2], 'val2': ['X', 'Y', 'Z']})
        
        merged = df1.merge(df2, on='id', how='inner')
        
        # Should have more records than df1 due to one-to-many relationship
        self.assertGreater(len(merged), len(df1))
    
    def test_merge_indicator(self):
        """Test using merge indicator to check join quality"""
        df1 = pd.DataFrame({'id': [1, 2, 3], 'val1': ['A', 'B', 'C']})
        df2 = pd.DataFrame({'id': [2, 3, 4], 'val2': ['X', 'Y', 'Z']})
        
        merged = df1.merge(df2, on='id', how='outer', indicator=True)
        
        both = merged[merged['_merge'] == 'both']
        left_only = merged[merged['_merge'] == 'left_only']
        right_only = merged[merged['_merge'] == 'right_only']
        
        self.assertEqual(len(both), 2)  # ids 2 and 3
        self.assertEqual(len(left_only), 1)  # id 1
        self.assertEqual(len(right_only), 1)  # id 4

class TestDataQualityMetrics(unittest.TestCase):
    """Test overall data quality metrics"""
    
    def test_completeness_score(self):
        """Test calculating data completeness"""
        df = pd.DataFrame({
            'col1': [1, 2, None, 4, 5],
            'col2': [10, None, None, 40, 50],
            'col3': [100, 200, 300, 400, 500]
        })
        
        total_cells = df.shape[0] * df.shape[1]
        null_cells = df.isnull().sum().sum()
        completeness = ((total_cells - null_cells) / total_cells) * 100
        
        self.assertEqual(completeness, 80.0)
    
    def test_uniqueness_score(self):
        """Test calculating uniqueness ratio"""
        df = pd.DataFrame({
            'id': [1, 1, 2, 3, 3, 3, 4, 5]
        })
        
        uniqueness = (df['id'].nunique() / len(df)) * 100
        self.assertEqual(uniqueness, 62.5)
    
    def test_validity_check(self):
        """Test checking value validity"""
        df = pd.DataFrame({
            'age': [25, 30, -5, 150, 40]
        })
        
        # Valid age range: 0-120
        valid_count = df[(df['age'] >= 0) & (df['age'] <= 120)].shape[0]
        validity_pct = (valid_count / len(df)) * 100
        
        self.assertEqual(validity_pct, 60.0)

if __name__ == '__main__':
    unittest.main(verbosity=2)
