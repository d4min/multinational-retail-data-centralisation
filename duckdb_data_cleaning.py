from duckdb_database_utils import DatabaseConnector
from duckdb_data_extraction import DataExtractor

import pandas as pd
import duckdb
import re

class DataCleaning:

    def __init__(self):
        self.duck_conn = duckdb.connect(":memory:")

    def clean_card_details(self):
        """
        Extracts and cleans card details from PDF using DuckDB for processing
        """

        db_extract = DataExtractor()
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        table = db_extract.retrieve_pdf_data(link)

        self.conn.register('card_details', table)

        card_providers = [
            'Diners Club / Carte',
            'Mastercard',
            'VISA 13 digit', 
            'VISA 16 digit',
            'Discover',
            'American Express',
            'Maestro',
            'JCB 16 digit',
            'VISA 19 digit',
            'JCB 15 digit'
        ]

        table = self.conn.execute(f"""
            SELECT *
            FROM card_details
            WHERE card_provider IN ({', '.join([f"'{provider}'"] for provider in card_providers)})
        """)

        # uses to_datetime() method to correct date entries and changes expiry_date and date_payment_confirmed columns to data type datetime
        table['expiry_date'] = pd.to_datetime(table['expiry_date'], format='%m/%y')
        table['expiry_date'] = table['expiry_date'].astype('datetime64[ns]')
        # changes the date format for the card expiry date to month/year as would be expected
        table['expiry_date'] = table['expiry_date'].dt.strftime('%m/%Y')

        table['date_payment_confirmed'] = pd.to_datetime(table['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')
        table['date_payment_confirmed'] = table['date_payment_confirmed'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['date_payment_confirmed'] = table['date_payment_confirmed'].dt.date

        db_connect = DataExtractor()
        db_connect.upload_to_db(table, 'dim_card_details')

    def clean_store_data(self):
        """
        Cleans store data retrieved through API using DuckDB
        """
        db_extract = DataExtractor()
        table = db_extract.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')

        self.conn.register('store_data', table)

        # Clean data using SQL - remove unwanted columns, fix continent, filter country codes
        table = self.conn.execute("""
            SELECT
                "index",
                store_code,
                 store_type,
                staff_numbers,
                opening_date,
                store_name,
                address,
                longitude,
                latitude,
                country_code,
                REPLACE(continent, 'ee', '') AS continent,
                locality
            FROM store_data
            WHERE country_code IN ('GB', 'US', 'DE')
        """).df()

        table.set_index('index', inplace=True)

        def remove_chars(value):
            return re.sub('[^0-9]+', '', str(value))
        
        table['staff_numbers'] = table['staff_numbers'].apply(remove_chars)
        table['staff_numbers'] = pd.to_numeric(table['staff_numbers'])


