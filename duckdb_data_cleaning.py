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
        
        # uses to_datetime() method to correct date entries in the opening_date column and changes the column data type to datetime
        table['opening_date'] = pd.to_datetime(table['opening_date'], infer_datetime_format=True, errors='coerce')
        table['opening_date'] = table['opening_date'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['opening_date'] = table['opening_date'].dt.date

        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_store_details')

    def convert_product_weights(self, table):
        """
        Convert product weights to a standardised kg format
        """
        self.conn.register('products_weight', table)

        # For complex string manipulations, pandas is still more convenient
        table['weight'] = table['weight'].astype(str)

        def convert_to_kg(value):
            value = re.sub('[^0123456789\.kgmlx]', '', value)

            try:
                # Handle different weight formats
                if 'kg' in value:
                    value = value.replace('kg', '')
                elif 'x' in value:
                    x_index = value.index('x')
                    value = value.replace('g', '')
                    value = float(value[:x_index]) * float(value[x_index + 1:])
                    value = value / 1000
                elif 'ml' in value:
                    value = value.replace('ml', '')
                    value = float(value) / 1000 
                elif 'g' in value and 'k' not in value:
                    value = value.replace('g', '')
                    value = float(value) / 1000
                
                return float(value)
            except (ValueError, TypeError):
                return None
        
        table['weight'] = table['weight'].apply(convert_to_kg)
        return table

    def clean_products_data(self):
        """
        Cleans product data using DuckDB for efficient processing        
        """
        db_extract = DataExtractor()
        table = db_extract.extract_from_s3('s3://data-handling-public/products.csv')

        self.conn.register('products', table)

        valid_categories = [
            'toys-and-games',
            'sports-and-leisure',
            'pets',
            'homeware',
            'health-and-beauty',
            'food-and-drink',
            'diy'
        ]

        table = self.conn.execute(f"""
            SELECT *
            FROM products
            WHERE category IN ({', '.join([f"'{cat}'" for cat in valid_categories])})
        """).df()

        table.index = table.index + 1

        # uses to_datetime() method to correct date entries for 'date_added' column and changes the datatype to datetime64'
        table['date_added'] = pd.to_datetime(table['date_added'], infer_datetime_format=True, errors='coerce')
        table['date_added'] = table['date_added'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['date_added'] = table['date_added'].dt.date

        # Clean weight column
        table = self.convert_product_weights(table)

        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_products')

    def clean_date_events_data(self):
        """
        Cleans date events data
        """
        table = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')

        self.conn.register('date_events', table)

        valid_periods = ['Evening', 'Midday', 'Morning', 'Late_Hours']

        table = self.conn.execute(f"""
            SELECT * 
            FROM date_events
            WHERE time_period IN ({', '.join([f"'{period}'" for period in valid_periods])})
        """).df()

         # changes the timestamp, month, day and year columns to datetime64[ns]
        table['timestamp'] = pd.to_datetime(table['timestamp'], infer_datetime_format=True, errors='coerce')
        table['timestamp'] = table['timestamp'].astype('datetime64[ns]')
        table['timestamp'] = table['timestamp'].dt.time

        table['month'] = pd.to_datetime(table['month'], format='%m')
        table['month'] = table['month'].astype('datetime64[ns]')
        table['month'] = table['month'].dt.month
        
        table['year'] = pd.to_datetime(table['year'], format='%Y')
        table['year'] = table['year'].astype('datetime64[ns]')
        table['year'] = table['year'].dt.year
        
        table['day'] = pd.to_datetime(table['day'], format='%d')
        table['day'] = table['day'].astype('datetime64[ns]')
        table['day'] = table['day'].dt.day

        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_date_times')


