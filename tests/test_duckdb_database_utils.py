import os
import pandas as pd
import yaml
import duckdb

class TestDatabaseConnector:
    def read_db_creds(self, creds_file):
        with open(creds_file, 'r') as db_creds:
            db_dict = yaml.safe_load(db_creds)
        return db_dict
    
    def init_db_engine(self):
        return duckdb.connect(':memory:')
    
    def upload_to_db(self, df, table_name):
        conn = duckdb.connect(':memory:')
        
        conn.register('temp_df', df)
        
        conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM temp_df")
        
        result = conn.execute(f"SELECT * FROM {table_name}").fetchall()
        return result

def test_read_db_creds():
    """Test reading database credentials from YAML file."""
    print("\n--- Testing read_db_creds method ---")
    
    test_creds = {
        'RDS_USER': 'test_user',
        'RDS_PASSWORD': 'test_password',
        'RDS_HOST': 'test_host',
        'RDS_PORT': '5432',
        'RDS_DATABASE': 'test_db'
    }
    
    with open('test_creds.yaml', 'w') as f:
        yaml.dump(test_creds, f)
    

    db_connector = TestDatabaseConnector()
    creds = db_connector.read_db_creds('test_creds.yaml')
    
    if creds['RDS_USER'] == 'test_user' and creds['RDS_PASSWORD'] == 'test_password':
        print("✓ Successfully read database credentials")
    else:
        print("✗ Failed to correctly read credentials")
    
    os.remove('test_creds.yaml')

def test_init_db_engine():
    """Test initializing a DuckDB connection."""
    print("\n--- Testing init_db_engine method ---")
    
    db_connector = TestDatabaseConnector()
    
    try:
        conn = db_connector.init_db_engine()
        
        result = conn.execute("SELECT 1 AS test").fetchall()
        
        if result[0][0] == 1:
            print("✓ Successfully initialized DuckDB connection")
        else:
            print("✗ Failed to execute simple query")
    except Exception as e:
        print(f"✗ Failed to initialize DuckDB connection: {e}")

def test_upload_to_db():
    """Test uploading a DataFrame to DuckDB."""
    print("\n--- Testing upload_to_db method ---")
    
    test_df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'value': [10.5, 20.3, 15.7]
    })
    
    db_connector = TestDatabaseConnector()
    
    try:
        result = db_connector.upload_to_db(test_df, 'test_table')
        
        if len(result) == 3:
            print("✓ Successfully uploaded DataFrame to DuckDB")
            print(f"Table contents: {result}")
        else:
            print(f"✗ Result contains {len(result)} rows, expected 3")
    except Exception as e:
        print(f"✗ Failed to upload DataFrame: {e}")

def run_all_tests():
    """Run all test functions."""
    print("=== STARTING TESTS FOR DUCKDB DATABASE UTILS ===")
    
    test_read_db_creds()
    test_init_db_engine()
    test_upload_to_db()
    
    print("\n=== ALL TESTS COMPLETED ===")

if __name__ == "__main__":
    run_all_tests()