from database_utils import *
from data_extraction import *

class DataCleaning:
    
    def clean_user_data(self):

        def clean_user_data(self):
        
            db_connect = DatabaseConnector()

            # creates a DataExtractor instance and calls the relevent method to extract the users table
            db_extract = DataExtractor()
            table =  db_extract.read_rds_table(db_connect, 'legacy_users')

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

            db_connect.upload_to_db(table, 'dim_users')

