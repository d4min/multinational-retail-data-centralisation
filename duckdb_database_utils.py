import yaml 
import duckdb 

class DatabaseConnector:

    def read_db_creds(self, creds_file):
        """
        Reads and returns database credentials from a yaml file into a python dictionary
        """
        with open(creds_file, 'r') as db_creds:
            db_dict = yaml.safe_load(db_creds)

            return db_dict
        
    def init_db_engine(self):
        """
        Create a DuckDB connection that can connect to postgres
        """
        db_creds = self.read_db_creds('aws_db_creds.yaml')
        
        # create DuckDB connection 
        conn = duckdb.connect(':memory:') # In memory DuckDB database

        conn.execute(f"""
        INSTALL postgres;
        LOAD postgres;
        ATTACH 'postgres://{db_creds["RDS_USER"]}:{db_creds["RDS_PASSWORD"]}@{db_creds["RDS_HOST"]}:{db_creds["RDS_PORT"]}/{db_creds["RDS_DATABASE"]}' 
        AS postgres_db;
        """)

        return conn
    
    def list_db_tables(self):
        """
        List all tables in the database
        """
        connection = self.init_db_engine()

        # List tables from the attached PostgreSQL database
        result = connection.execute("SELECT table_name FROM postgres_db.information_schema.tables WHERE table_schema = 'public'").fetchall()

        # Extract table names from the result
        table_names = [row[0] for row in result]

        print(table_names)

    def upload_to_db(self, df, table_name):
        """
        Use duckdb to upload cleaned pandas DataFrame to PostgreSQL
        """
        
        db_creds = self.read_db_creds('sales_data_creds.yaml')

        # Create connection string for PostgreSQL
        postgres_conn_string = f"postgres://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"
        
        # Create DuckDB connection
        conn = duckdb.connect(':memory:')
        
        # Register the pandas DataFrame as a view in DuckDB
        conn.register('temp_df', df)
        
        # Create or replace the table in PostgreSQL using DuckDB's COPY functionality
        conn.execute(f"""
            COPY (SELECT * FROM temp_df) 
            TO '{postgres_conn_string}' 
            (FORMAT POSTGRES, TABLE '{table_name}', CREATE_TABLE TRUE, OVERWRITE_OR_IGNORE TRUE)
        """)
