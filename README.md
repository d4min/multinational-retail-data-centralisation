# DuckDB Implementation 

## Project Overview 
This project explores the performance differences between Pandas and DuckDB in handling data processing tasks for a multinational retail company's data centralisation efforts. The original implementation usees Pandas, SQLAlchemy and direct PostgreSQL connections, while the new implementation leverages DuckDB's capabilities for potentially improved performance. 

## Project Goals 
- Compare execution speed between Pandas and DuckDB implementations 
- Analyse memory usage and resource efficiency

## Implementation Comparison

### 1. Database Utilities (database_utils.py vs duckdb_database_utils.py)

#### Key Differences:
1. **Connection Handling**:
```python
# Original: Uses SQLAlchemy and psycopg2 for PostgreSQL connections
engine = create_engine(f"""
postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}
""")

# DuckDB: Uses DuckDB's built-in PostgreSQL connector, potentially reducing overhead
conn = duckdb.connect(':memory:') # In memory DuckDB database

conn.execute(f"""
ATTACH 'postgres://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}' 
AS postgres_db
""")
```
2. **Data Upload Method**:
```python
# Original: Uses Pandas' to_sql() method via SQLAlchemy
df.to_sql(table_name, engine, if_exists='replace', index=False, index_label='index')

# DuckDB: Utilises DuckDB's COPY command, which can be more efficient for large datasets
conn.register('temp_df', df)
        
conn.execute(f"""
    COPY (SELECT * FROM temp_df) 
    TO '{postgres_conn_string}' 
    (FORMAT POSTGRES, TABLE '{table_name}', CREATE_TABLE TRUE, OVERWRITE_OR_IGNORE TRUE)
    """)
```