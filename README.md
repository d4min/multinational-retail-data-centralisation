# Multinational Retail Data Centralisation

In this project I will act as an employee for a multinational company  that sells various goods across the globe.

Currently, their sales data is spread across many different data sources making it not easily accesible or analysable by current members of the team. 

In an effort to become more data-driven the organisation would like to make its sales data accessible from one centralised location.

My first goal will be to produce a system that stores the current company data in a database so it's accessed from one centralised location and acts as a single source of trust for sales data. 

I will then query the database to get up-to-date metrics for business. 

## Milestone 1 

- Creates a remote github repository for this project to version control the software.

- Connects the remote repository to a local clone using the command line.

```bash
git clone git clone https://github.com/d4min/multinational-retail-data-centralisation.git
```

## Milestone 2 Task 1, 2, 3

- Set up a database 'sales_data' within pgadmin4. This database will store all the company information once I extract it from various data sources. 

- Initialised the three classes which will be used in this project in three seperate python scripts: 

    1. The DataExtractor class will be responsible for methods used to extract data from the data sources. The sources will include a  CSV file, an API and an S3 bucket.

    1. The DatabaseConnector class which will be used to connect to the database and upload the cleaned data ready for analysis.

    1. The DataCleaning class which will include methods to clean the data from the various sources. 

- Created the following methods in the DatabaseConnector class which will be used to connect and retrieve data from the AWS database:

    1. read_db_creds(): read the AWS database credentials from a YAML file and return a python dictionary of said credentials. 

    1. init_db_engine(): uses the read_db_cred() method to retrieve database credentials and uses these to initialise and return a SQLalchemy engine cononecting to the database.

    1. list_db_tables(): lists the tables present in the AWS database.

    1. upload_to_db(): takes in a pandas dataframe and table name and uploads them to the local sales_db database setup within pgadmin4

- Created the following method in the DatabaseExtractor class: 

    1. read_rds_table(): extracts the database table to a pandas dataframe. 

- Created the following method in the DataCleaning class:

    1. clean_user_data(): performs the cleaning of the user data. Taking into consideration NULL values, errors with dates, incomplete rows and rows filled with the wrong information.

## Milestone 2 Task 4

- Created the method 'retrieve_pdf_data' in the DataExtractor class which uses the tabula-py library to extract data from a pdf file into a pandas dataframe. 

```python
def retrive_pdf_data(self, link):

        table = tabula.read_pdf(link, pages='all')
        table_df = pd.concat(table)

        return table_df
```
- Created the method 'clean_card_details' in the DataCleaning class to clean the card details data so it is ready to be uploaded to the postgres database.

- Used the upload_to_db method in the DatabaseConnector class to store the cleaned card details in the postgres database

```python
def upload_to_db(self, df, table_name):
        
        # uses read_db_creds method to read the sales_data database credentials
        db_creds = self.read_db_creds('sales_data_creds.yaml')

        engine = create_engine(f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        df.to_sql(table_name, engine, if_exists='replace', index=False, index_label='index')

db_connect = DatabaseConnector()
db_connet.upload_to_db(table, 'dim_card_details)
```

 





