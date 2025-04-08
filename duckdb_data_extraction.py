from duckdb_database_utils import DatabaseConnector
import duckdb
import pandas as pd
import tabula
import requests
import json
import boto3

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

