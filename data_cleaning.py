from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd

class DataCleaning:
    
    def clean_user_data(self):
    
        db_connect = DatabaseConnector()

        # creates a DataExtractor instance and calls the relevent method to extract the users table
        db_extract = DataExtractor()
        table =  db_extract.read_dbs_table(db_connect, 'legacy_users')

        # changes country_code and country columns data type to category and replaces mis-inputs to match the relevent categories.
        table['country_code'] = table['country_code'].astype('category')
        table['country'] = table['country'].astype('category')
        table['country_code'].replace('GGB', 'GB', inplace=True)

        # drops rows filled with nulls and the wrong values
        country_codes = {'GB', 'US', 'DE'}
        inconsistent_categories = set(table['country_code']) - country_codes
        inconsistent_rows = table['country_code'].isin(inconsistent_categories)
        table = table[~inconsistent_rows]

        # uses to_datetime() method to correct date entries for D.O.B column and join_date column and changes the datatype to datetime64
        table['date_of_birth'] = pd.to_datetime(table['date_of_birth'], infer_datetime_format=True, errors='coerce')
        table['date_of_birth'] = table['date_of_birth'].astype('datetime64[ns]')

        table['join_date'] = pd.to_datetime(table['join_date'], infer_datetime_format=True, errors='coerce')
        table['join_date'] = table['join_date'].astype('datetime64[ns]')

        # uploads the cleaned user details to the local postgres database
        db_connect.upload_to_db(table, 'dim_users')

    def clean_card_details(self):

        # calls the retreive_pdf_data method with a link to the card_details pdf as an argument. This returns a df of the pdf. 
        db_extract = DataExtractor()
        link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        table = db_extract.retrieve_pdf_data(link)


        # changes card_provider columns data type to category
        table['card_provider'] = table['card_provider'].astype('category')

        
        # defines a set of the valid card providers compares them against categories present in the table and removes the inconsistent rows. 
        card_providers = {'Diners Club / Carte Blanche', 'Mastercard', 'VISA 13 digit', 'VISA 16 digit', 'Discover', 'American Express', 'Maestro', 'JCB 16 digit', 'VISA 19 digit', 'JCB 15 digit'}
        inconsistent_categories = set(table['card_provider']) - card_providers
        inconsistent_rows = table['card_provider'].isin(inconsistent_categories)
        table = table[~inconsistent_rows]

        
        # uses to_datetime() method to correct date entries and changes expiry_date and date_payment_confirmed columns to data type datetime

        table['expiry_date'] = pd.to_datetime(table['expiry_date'], format='%m/%y')
        table['expiry_date'] = table['expiry_date'].astype('datetime64[ns]')
        
        table['date_payment_confirmed'] = pd.to_datetime(table['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')
        table['date_payment_confirmed'] = table['date_payment_confirmed'].astype('datetime64[ns]')

        
        # uploads the cleaned user details to the local postgres database
        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_card_details')
    










