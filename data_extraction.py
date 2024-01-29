from database_utils import *  
import pandas as pd

class DataExtractor:

    def read_dbs_table(self, db_connector, table):
        engine = db_connector.init_db_engine()

        query = (f"SELECT * FROM {table}")
        table_df = pd.read_sql_query(sql=text(query), con=engine.connect())
        
        return table_df
    


