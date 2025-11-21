"""
Root Cause Analysis Module
Identifies and analyzes data quality issues and mismatches
"""

import pandas as pd
from logging_utils import log_action, log_error

def identify_mismatched_records(raw_df, transformed_df, key_column):
    """Identify records that differ between raw and transformed data"""
    log_action(f"Identifying mismatched records on {key_column}...")
    
    raw_keys = set(raw_df[key_column].unique())
    transformed_keys = set(transformed_df[key_column].unique())
    
    missing_keys = raw_keys - transformed_keys
    extra_keys = transformed_keys - raw_keys
    
    analysis = {
        'missing_in_transformed': list(missing_keys),
        'extra_in_transformed': list(extra_keys),
        'missing_count': len(missing_keys),
        'extra_count': len(extra_keys)
    }
    
    if missing_keys:
        log_error(f"Found {len(missing_keys)} records missing in transformed data")
    if extra_keys:
        log_error(f"Found {len(extra_keys)} extra records in transformed data")
    
    return analysis

def compare_raw_vs_transformed(raw_df, transformed_df, sample_size=5):
    """Compare raw and transformed datasets to identify changes"""
    log_action("Comparing raw vs transformed data...")
    
    # Count differences
    raw_count = len(raw_df)
    transformed_count = len(transformed_df)
    
    comparison = {
        'raw_record_count': raw_count,
        'transformed_record_count': transformed_count,
        'records_added': max(0, transformed_count - raw_count),
        'records_removed': max(0, raw_count - transformed_count),
        'null_values_raw': raw_df.isnull().sum().sum(),
        'null_values_transformed': transformed_df.isnull().sum().sum()
    }
    
    log_action(f"Raw records: {raw_count}, Transformed records: {transformed_count}")
    log_action(f"Null values - Raw: {comparison['null_values_raw']}, Transformed: {comparison['null_values_transformed']}")
    
    return comparison

def detect_foreign_key_problems(parent_df, child_df, parent_key, child_key):
    """Detect foreign key constraint violations"""
    log_action(f"Detecting FK problems: {child_key} -> {parent_key}...")
    
    parent_keys = set(parent_df[parent_key].unique())
    child_keys = child_df[child_key].unique()
    
    orphaned_records = []
    for key in child_keys:
        if key not in parent_keys:
            orphaned_count = len(child_df[child_df[child_key] == key])
            orphaned_records.append({
                'foreign_key_value': key,
                'orphaned_record_count': orphaned_count
            })
    
    fk_analysis = {
        'total_orphaned_keys': len(orphaned_records),
        'orphaned_records': orphaned_records[:10],  # Sample first 10
        'parent_key_count': len(parent_keys),
        'child_key_count': len(set(child_keys))
    }
    
    if orphaned_records:
        log_error(f"Found {len(orphaned_records)} foreign key violations")
    else:
        log_action("✓ No foreign key violations detected")
    
    return fk_analysis

def analyze_data_distribution(df, column_name):
    """Analyze distribution of values in a column"""
    log_action(f"Analyzing distribution of {column_name}...")
    
    distribution = {
        'column': column_name,
        'total_records': len(df),
        'unique_values': df[column_name].nunique(),
        'null_count': df[column_name].isnull().sum(),
        'top_values': df[column_name].value_counts().head(10).to_dict()
    }
    
    return distribution

def run_root_cause_analysis(customers_raw, products_raw, sales_raw,
                            customers_transformed, products_transformed, sales_transformed):
    """Execute complete root cause analysis"""
    log_action("=" * 60)
    log_action("STARTING ROOT CAUSE ANALYSIS")
    log_action("=" * 60)
    
    rca_results = {}
    
    # Compare raw vs transformed
    rca_results['customers_comparison'] = compare_raw_vs_transformed(customers_raw, customers_transformed)
    rca_results['products_comparison'] = compare_raw_vs_transformed(products_raw, products_transformed)
    rca_results['sales_comparison'] = compare_raw_vs_transformed(sales_raw, sales_transformed)
    
    # Identify mismatched records
    rca_results['customer_mismatches'] = identify_mismatched_records(
        customers_raw, customers_transformed, 'customer_id'
    )
    rca_results['product_mismatches'] = identify_mismatched_records(
        products_raw, products_transformed, 'product_id'
    )
    rca_results['sales_mismatches'] = identify_mismatched_records(
        sales_raw, sales_transformed, 'sale_id'
    )
    
    # Detect FK problems
    rca_results['customer_fk_analysis'] = detect_foreign_key_problems(
        customers_transformed, sales_transformed, 'customer_id', 'customer_id'
    )
    rca_results['product_fk_analysis'] = detect_foreign_key_problems(
        products_transformed, sales_transformed, 'product_id', 'product_id'
    )
    
    # Analyze distributions
    rca_results['sales_status_distribution'] = analyze_data_distribution(sales_transformed, 'status')
    rca_results['product_category_distribution'] = analyze_data_distribution(products_transformed, 'category')
    
    log_action("=" * 60)
    log_action("ROOT CAUSE ANALYSIS COMPLETE")
    log_action("=" * 60)
    
    return rca_results

if __name__ == '__main__':
    # Load raw data
    customers_raw = pd.read_csv('data_customers.csv')
    products_raw = pd.read_csv('data_products.csv')
    sales_raw = pd.read_csv('data_sales.csv')
    
    # For demo, use raw as transformed (in real scenario, load transformed data)
    customers_transformed = customers_raw.copy()
    products_transformed = products_raw.copy()
    sales_transformed = sales_raw.copy()
    
    results = run_root_cause_analysis(
        customers_raw, products_raw, sales_raw,
        customers_transformed, products_transformed, sales_transformed
    )
    
    print("\nRoot Cause Analysis Results:")
    for key, value in results.items():
        print(f"\n{key}:")
        print(value)
