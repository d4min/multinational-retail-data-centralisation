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


