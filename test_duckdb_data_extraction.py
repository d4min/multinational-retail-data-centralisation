import pytest
import pandas as pd
import duckdb
from duckdb_data_extraction import DataExtractor
from duckdb_database_utils import DatabaseConnector

def test_read_dbs_table():
    """Test reading a table from the database"""
    print("\n--- Testing read_dbs_table method ---")
    
    extractor = DataExtractor()
    connector = DatabaseConnector()
    
    try:
        # Test with a known table name
        df = extractor.read_dbs_table(connector, 'legacy_users')
        if isinstance(df, pd.DataFrame):
            print("✓ Successfully read table from database")
            print(f"DataFrame shape: {df.shape}")
        else:
            print("✗ Failed to return a DataFrame")
    except Exception as e:
        print(f"✗ Error reading table: {e}")

def test_retrieve_pdf_data():
    """Test retrieving data from a PDF"""
    print("\n--- Testing retrieve_pdf_data method ---")
    
    extractor = DataExtractor()
    test_pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    
    try:
        df = extractor.retrieve_pdf_data(test_pdf_url)
        if isinstance(df, pd.DataFrame):
            print("✓ Successfully retrieved PDF data")
            print(f"DataFrame shape: {df.shape}")
        else:
            print("✗ Failed to return a DataFrame")
    except Exception as e:
        print(f"✗ Error retrieving PDF: {e}")

def test_list_number_of_stores():
    """Test retrieving the number of stores"""
    print("\n--- Testing list_number_of_stores method ---")
    
    extractor = DataExtractor()
    test_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    
    try:
        connector = DatabaseConnector()
        num_stores = extractor.list_number_of_stores(test_endpoint)
        if isinstance(num_stores, int) and num_stores > 0:
            print(f"✓ Successfully retrieved number of stores: {num_stores}")
        else:
            print("✗ Failed to retrieve valid number of stores")
    except Exception as e:
        print(f"✗ Error getting store count: {e}")

def test_retrieve_stores_data():
    """Test retrieving store data"""
    print("\n--- Testing retrieve_stores_data method ---")
    
    extractor = DataExtractor()
    test_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    
    try:
        df = extractor.retrieve_stores_data(test_endpoint)
        if isinstance(df, pd.DataFrame):
            print("✓ Successfully retrieved stores data")
            print(f"DataFrame shape: {df.shape}")
        else:
            print("✗ Failed to return a DataFrame")
    except Exception as e:
        print(f"✗ Error retrieving stores data: {e}")

def test_extract_from_s3():
    """Test extracting data from S3"""
    print("\n--- Testing extract_from_s3 method ---")
    
    extractor = DataExtractor()
    test_s3_address = 's3://data-handling-public/products.csv'
    
    try:
        df = extractor.extact_from_s3(test_s3_address)
        if isinstance(df, pd.DataFrame):
            print("✓ Successfully extracted data from S3")
            print(f"DataFrame shape: {df.shape}")
        else:
            print("✗ Failed to return a DataFrame")
    except Exception as e:
        print(f"✗ Error extracting from S3: {e}")

def run_all_tests():
    """Run all test functions"""
    print("=== STARTING TESTS FOR DUCKDB DATA EXTRACTION ===")
    
    test_read_dbs_table()
    test_retrieve_pdf_data()
    test_list_number_of_stores()
    test_retrieve_stores_data()
    test_extract_from_s3()
    
    print("\n=== ALL TESTS COMPLETED ===")

if __name__ == "__main__":
    run_all_tests()