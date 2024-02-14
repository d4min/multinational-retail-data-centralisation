from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text, inspect
import pandas as pd
import tabula
import requests
import json

class DataExtractor:

    # reads in a specified table from the AWS database as a panda's dataframe
    def read_dbs_table(self, db_connector, table):
        engine = db_connector.init_db_engine()

        query = (f"SELECT * FROM {table}")
        table_df = pd.read_sql_query(sql=text(query), con=engine.connect())
        
        return table_df
    
    # reads in data from a specified pdf file as a panda's  dataframe
    def retrieve_pdf_data(self, link):

        table = tabula.read_pdf(link, pages='all')
        table_df = pd.concat(table)

        return table_df

    # sends an api get request to retrieve the number of stores 
    def list_number_of_stores(self, endpoint):

        url = endpoint 

        # reads in the x-api-key needed for authorisation and saves it as 'headers' which will be sent with the request
        connect = DatabaseConnector()
        headers = connect.read_db_creds('api_key.yaml')

        response = requests.get(url, headers=headers)

        data = response.json()
        number_of_stores = data['number_stores']

        return number_of_stores
    
    # retrieves each stores data and saves them in a pandas dataframe
    def retrieve_stores_data(self, endpoint):

        url = endpoint

        connect = DatabaseConnector()
        headers = connect.read_db_creds('api_key.yaml')

        # calls the list_number_of_stores method to retrieve the amount of stores 
        number_of_stores = self.list_number_of_stores('https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores')
        
        # empty list which will store each of the store details as a dictionory. This will be used to create the dataframe
        stores_list = []

        # sends a get request for each store 
        for i in range(number_of_stores):
            
            # replaces the parameter '{store_number}' for i which is a store index
            new_url = url.replace('{store_number}', str(i))
            response = requests.get(new_url, headers=headers)
            data = response.json() 
            stores_list.append(data)

        # creates a pandas dataframe from the list of dictionaries storing store details 
        store_data_df = pd.DataFrame(stores_list)

        return store_data_df






