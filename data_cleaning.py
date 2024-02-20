from database_utils import DatabaseConnector
from data_extraction import DataExtractor
import pandas as pd
import re

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
        # removes timestamp from column as only the date is required 
        table['date_of_birth'] = table['date_of_birth'].dt.date

        table['join_date'] = pd.to_datetime(table['join_date'], infer_datetime_format=True, errors='coerce')
        table['join_date'] = table['join_date'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['join_date'] = table['join_date'].dt.date

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
        # changes the date format for the card expiry date to month/year as would be expected
        table['expiry_date'] = table['expiry_date'].dt.strftime('%m/%Y')

        
        table['date_payment_confirmed'] = pd.to_datetime(table['date_payment_confirmed'], infer_datetime_format=True, errors='coerce')
        table['date_payment_confirmed'] = table['date_payment_confirmed'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['date_payment_confirmed'] = table['date_payment_confirmed'].dt.date

        # uploads the cleaned user details to the local postgres database
        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_card_details')


    # cleans the store data retrieved through an API 
    def clean_store_data(self):
        
        # creates an instance of the DataExtractor class which includes the methods required to extract store data
        data_extractor = DataExtractor()
        table = data_extractor.retrieve_stores_data('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}')

        # sets the index of the pandas dataframe 
        table.set_index('index', inplace=True)

        # removes 'lat' column which is not needed and is only filled with null values 
        table.drop('lat', axis=1, inplace=True)

        # some of the values in the continent column have an error where they begin with 'ee' but are otherwise correct. This removes the 'ee' substring from those rows. 
        table['continent'] = table['continent'].str.replace('ee', '')

        # changes 'country_code', 'continent' and 'store_type' columns data type to category
        table['country_code'] = table['country_code'].astype('category')
        table['continent'] = table['continent'].astype('category')
        table['store_type'] = table['store_type'].astype('category')

        # Defines a set of valid country codes and removes rows where the column entry does not match these. This removes rows filled with null values and incorrect data. 
        country_codes = {'GB', 'US', 'DE'}
        inconsistent_categories = set(table['country_code']) - country_codes
        inconsistent_rows = table['country_code'].isin(inconsistent_categories)
        table = table[~inconsistent_rows]   

        # removes any alphabetical characters from rows in the staff_numbers column using a regular expression so they are ready to be converted to data type int
        table['staff_numbers'] = table['staff_numbers'].str.replace(r"[a-zA-z]", '')

        # changes the staff_numbers data type to numberic so it can used for calculations
        table['staff_numbers'] = pd.to_numeric(table['staff_numbers'])

        # uses to_datetime() method to correct date entries in the opening_date column and changes the column data type to datetime
        table['opening_date'] = pd.to_datetime(table['opening_date'], infer_datetime_format=True, errors='coerce')
        table['opening_date'] = table['opening_date'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['opening_date'] = table['opening_date'].dt.date

        # creates an instance of the DatabaseConnector class to upload the cleaned table to postgres
        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_store_details')

    #  A function to clean the weight series from the dataframe so it is usable for calculations.
    def convert_product_weights(self, table):
        
        # sets the column data type to string so because string methods are needed for some of the cleaning
        table['weight'] = table['weight'].astype(str)
        
        # this is the method that will be used in the pd.apply() call
        def convert_to_kg(value): 
            
            # some of the weight entries in the table dealing with food items are stored as '12 x 100g'. This if statement deals with them, first by removing the 'g' and then multiplying to get the total weight and then converting to kg
            if 'x' in value:

                x_index = value.index('x')
                value = value.replace('g', '')
                value = int(value[:x_index - 1]) * int(value[x_index + 2:])
                value = float(value) / 1000
                
                value = str(value)

            # there are some weight entries with decimal points in weird places e.g. '77  .' which causes an error when trying to treat the value as a float due to the spaces between the integer and the decimal point. This regular expression removes everything but digits, decimal points and the weight metrics from the string. 
            value = re.sub('[^0123456789\.kgml]', '', value)
            
            # this if statement removes the weight metric and converts ml and g to kg.
            if 'kg' in value:
               value = value.replace('kg', '')
            elif 'ml' in value:
                value = value.replace('ml', '')
                value = float(value) / 1000 
            elif 'g' in value and 'k' not in value:
                value = value.replace('g', '')
                value = float(value) / 1000

            
            return value 

        table['weight'] = table['weight'].apply(convert_to_kg)
        table['weight'] = table['weight'].astype(float)
    
        return table

    # cleans the products data and stores it in our local database.
    def clean_products_data(self):

        # creates a dataextractor instance which calls the extract_from_s3 method from that class
        extractor = DataExtractor()
        table = extractor.extract_from_s3('s3://data-handling-public/products.csv')

        # original index was zero based, used this to change it to start at 1 
        table.index = table.index + 1

        # changes the 'category' and 'removed' tables to type category 
        table['category'] = table['category'].astype('category')
        table['removed'] = table['removed'].astype('category')

        # defines a set of valid product categories and removes rows with inconsistent values. 
        categories = {'toys-and-games', 'sports-and-leisure', 'pets', 'homeware', 'health-and-beauty', 'food-and-drink', 'diy'}
        inconsistent_categories = set(table['category']) - categories
        inconsistent_rows = table['category'].isin(inconsistent_categories)
        table = table[~inconsistent_rows]

        # uses to_datetime() method to correct date entries for 'date_added' column and changes the datatype to datetime64'
        table['date_added'] = pd.to_datetime(table['date_added'], infer_datetime_format=True, errors='coerce')
        table['date_added'] = table['date_added'].astype('datetime64[ns]')
        # removes timestamp from column as only the date is required 
        table['date_added'] = table['date_added'].dt.date
        
        # calls the method which cleans the weights column
        table = self.convert_product_weights(table)
        
        # uploads the cleaned table to postgres
        db_connect = DatabaseConnector()
        db_connect.upload_to_db(table, 'dim_products')








        


    










