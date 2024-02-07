from database_utils import DatabaseConnector
from sqlalchemy import create_engine, text, inspect
import pandas as pd
import tabula

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


