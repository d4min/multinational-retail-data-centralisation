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

## Milestone 2

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


 





