"""
Logging Utilities Module
Provides centralized logging functionality for ETL operations
"""

import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

logger = logging.getLogger(__name__)

def log_action(message):
    """Log informational action messages"""
    logger.info(message)

def log_error(message):
    """Log error messages"""
    logger.error(message)

def log_warning(message):
    """Log warning messages"""
    logger.warning(message)

def log_duration(operation, duration_seconds):
    """Log operation duration"""
    logger.info(f"{operation} completed in {duration_seconds:.2f} seconds")

def log_query(query, duration_seconds=None):
    """Log SQL query execution"""
    logger.info(f"Executed query: {query[:100]}...")
    if duration_seconds:
        logger.info(f"Query duration: {duration_seconds:.2f} seconds")

def get_timestamp():
    """Get current timestamp string"""
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
