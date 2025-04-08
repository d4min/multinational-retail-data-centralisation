from duckdb_database_utils import DatabaseConnector
import duckdb
import pandas as pd
import tabula
import requests
import json
import boto3
import io

class DataExtractor:

    def read_dbs_table(self, db_connector, table):
        """
        Reads in a specified table from the AWS database as a Panda's DataFrame.
        """
        connection = db_connector.init_db_engine()

        # Query the table through DuckDB's SQL interface allowing for optimissation of query performance
        result = connection.execute(f"SELECT * FROM postgres_db.{table}").df()
        
        return result

    def retrieve_pdf_data(self, link):
        """
        Reads in data from a specified pdf file as a Panda's DataFrame.

        This function remains largely the same as DuckDB doesn't have native pdf parsing capabilities.
        """
        table = tabula.read_pdf(link, pages="all")
        table_df = pd.concat(table)

        # Convert to DuckDB DataFrame after extraction
        connection = duckdb.connect(':memory:')

        connection.register('pdf_table_view', table_df)

        result = connection.execute("SELECT * FROM pdf_table_view").df()
    
        return result
    
    def list_number_of_stores(self, endpoint):
        """
        Sends an API request to retrieve the number of stores
        """
        url = endpoint

        connect = DatabaseConnector()
        headers = connect.read_db_creds('api_key.yaml')

        response = requests.get(url, headers=headers)

        data = response.json()
        number_of_stores = data['number_stores']

        return number_of_stores
    
    def retrieve_stores_data(self, endpoint):
        """
        Retrieves each stores data and saves them in a DuckDB DataFrame
        """
        url = endpoint 

        connect = DatabaseConnector()
        headers = connect.read_db_creds('api_key.yaml')

        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')

        stores_list = []

        for i in range(number_of_stores):
            new_url = url.replace('{store_number}', str(i))
            response = requests.get(new_url, headers=headers)
            data = response.json()
            stores_list.append(data)
        
        stores_df = pd.DataFrame(stores_list)

        # Convert to DuckDB DataFrame
        conn = duckdb.connect(':memory:')
        duckdb_df = conn.from_df(stores_df)

        return duckdb_df.df()
    
    def extact_from_s3(self, s3_address):
        """
        Uses boto3 to extract a CSV file from an s3 bucket and loads it using DuckDB
        """
        address_list = s3_address.split('/')
        bucket = address_list[-2]
        key = address_list[-1]

        s3 = boto3.client('s3')

        # Instead of downloading to disk, get object and read directly
        response = s3.get_object(Bucket=bucket, Key=key)
        # Read the CSV content into a pandas DataFrame first
        df = pd.read_csv(io.BytesIO(response['Body'].read()))
    
        # Then convert to DuckDB
        conn = duckdb.connect(':memory:')
        conn.register('temp_df', df)

        products_df = conn.execute("SELECT * FROM temp_df").df()

        return products_df
    
         


        


    
    




