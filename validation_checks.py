"""
Data Validation Module
Performs comprehensive validation checks on extracted and transformed data
"""

import pandas as pd
from logging_utils import log_action, log_error

def check_null_values(df, df_name):
    """Check for null values in dataframe"""
    log_action(f"Checking null values in {df_name}...")
    null_counts = df.isnull().sum()
    
    if null_counts.sum() > 0:
        log_error(f"{df_name} has null values:")
        for col, count in null_counts[null_counts > 0].items():
            log_error(f"  - {col}: {count} null values")
        return False
    else:
        log_action(f"✓ No null values in {df_name}")
        return True

def check_duplicates(df, df_name, key_column):
    """Check for duplicate records based on key column"""
    log_action(f"Checking duplicates in {df_name} on column '{key_column}'...")
    duplicates = df[df.duplicated(subset=[key_column], keep=False)]
    
    if len(duplicates) > 0:
        log_error(f"{df_name} has {len(duplicates)} duplicate records on {key_column}")
        return False
    else:
        log_action(f"✓ No duplicates in {df_name}")
        return True

def check_schema_mismatch(df1, df2, df1_name, df2_name):
    """Check if two dataframes have matching schemas"""
    log_action(f"Checking schema match between {df1_name} and {df2_name}...")
    
    df1_cols = set(df1.columns)
    df2_cols = set(df2.columns)
    
    if df1_cols != df2_cols:
        missing_in_df2 = df1_cols - df2_cols
        missing_in_df1 = df2_cols - df1_cols
        
        if missing_in_df2:
            log_error(f"Columns in {df1_name} but not in {df2_name}: {missing_in_df2}")
        if missing_in_df1:
            log_error(f"Columns in {df2_name} but not in {df1_name}: {missing_in_df1}")
        return False
    else:
        log_action(f"✓ Schema match between {df1_name} and {df2_name}")
        return True

def check_key_integrity(parent_df, child_df, parent_key, child_key, parent_name, child_name):
    """Check referential integrity between parent and child tables"""
    log_action(f"Checking referential integrity: {child_name}.{child_key} -> {parent_name}.{parent_key}...")
    
    parent_keys = set(parent_df[parent_key].unique())
    child_keys = set(child_df[child_key].unique())
    
    orphaned_keys = child_keys - parent_keys
    
    if orphaned_keys:
        log_error(f"Found {len(orphaned_keys)} orphaned records in {child_name}")
        log_error(f"  Sample orphaned keys: {list(orphaned_keys)[:5]}")
        return False
    else:
        log_action(f"✓ Referential integrity maintained")
        return True

def check_join_mismatches(merged_df, original_df, merged_name, original_name):
    """Check for data loss after joins"""
    log_action(f"Checking join integrity between {original_name} and {merged_name}...")
    
    original_count = len(original_df)
    merged_count = len(merged_df)
    
    if merged_count < original_count:
        lost_records = original_count - merged_count
        log_error(f"Join resulted in data loss: {lost_records} records lost")
        return False
    elif merged_count > original_count:
        extra_records = merged_count - original_count
        log_error(f"Join resulted in duplicate records: {extra_records} extra records")
        return False
    else:
        log_action(f"✓ Join integrity maintained ({merged_count} records)")
        return True

def validate_data_quality(customers, products, sales):
    """Run all validation checks"""
    log_action("=" * 60)
    log_action("STARTING DATA VALIDATION")
    log_action("=" * 60)
    
    all_checks_passed = True
    
    # Null value checks
    all_checks_passed &= check_null_values(customers, "customers")
    all_checks_passed &= check_null_values(products, "products")
    all_checks_passed &= check_null_values(sales, "sales")
    
    # Duplicate checks
    all_checks_passed &= check_duplicates(customers, "customers", "customer_id")
    all_checks_passed &= check_duplicates(products, "products", "product_id")
    all_checks_passed &= check_duplicates(sales, "sales", "sale_id")
    
    # Key integrity checks
    all_checks_passed &= check_key_integrity(customers, sales, "customer_id", "customer_id", "customers", "sales")
    all_checks_passed &= check_key_integrity(products, sales, "product_id", "product_id", "products", "sales")
    
    log_action("=" * 60)
    if all_checks_passed:
        log_action("✓ ALL VALIDATION CHECKS PASSED")
    else:
        log_action("✗ SOME VALIDATION CHECKS FAILED")
    log_action("=" * 60)
    
    return all_checks_passed

if __name__ == '__main__':
    # Load data from CSV for validation
    customers = pd.read_csv('data_customers.csv')
    products = pd.read_csv('data_products.csv')
    sales = pd.read_csv('data_sales.csv')
    
    validate_data_quality(customers, products, sales)
