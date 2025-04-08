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

### 2. Data Extraction (data_extraction.py vs duckdb_data_extraction.py)

#### Key Differences:
1. **API Data Handling**:
- Original: Uses Pandas DataFrame for storing API responses
- DuckDB: Initially cretes Pandas DataFrame then converts to DuckDB
- Future Optimisation: Could directly insert API responses into DuckDB tables using SQL operations. 

```python
# Original Pandas Implementation
stores_df = pd.DataFrame(stores_list)

# DuckDB Implementation
stores_df = pd.DataFrame(stores_list)
conn = duckdb.connect(':memory:')
duckdb_df = conn.from_df(stores_df)
```

### 3. Data Cleaning (data_cleaning.py vs duckdb_data_cleaning.py)

#### Key Differences:

1. **SQL-Based Filtering**:
```python
# Original Pandas: Filterinconsistent_categories = set(table['country_code']) - country_codes
inconsistent_rows = table['country_code'].isin(inconsistent_categories)
table = table[~inconsistent_rows]

# DuckDB: Use SQL for filtering
table = self.conn.execute(f"""
    SELECT *
    FROM store_data
    WHERE country_code IN ('GB', 'US', 'DE')
""").df()
```

2. **Data Type Handling**:
```python
# Original Pandas: Chain multiple operations
table['country_code'] = table['country_code'].astype('category')
table['continent'] = table['continent'].astype('category')
table['store_type'] = table['store_type'].astype('category')

# DuckDB optimisation
table = self.conn.execute("""
    SELECT 
        TRY_CAST(date_added AS DATE) as date_added,
        -- other columns
    FROM products
""").df()
```

3. **String Operations**:
```python
# Original Pandas: Use pandas string methods
table['continent'] = table['continent'].str.replace('ee', '')

# DuckDB: Use SQL string functions
table = self.conn.execute("""
    SELECT
        REPLACE(continent, 'ee', '') AS continent,
        -- other columns
    FROM store_data
""").df()
```

4. **Hybrid Approach**:
- The DuckDB implementation uses a hybrid approach:
    - SQL operations for filtering and simple transformations
    - Pandas operations for complex sting manipulations and date handling 
    - In-memory DuckDB for intermediate operations 

## Performance Results 

The benchmark testing revealed modest performance differences on my dataset size:

| Method | Pandas Time | DuckDB Time | Speedup
|----------|----------|----------|----------|
| Extract Users    | 0.856s    | 0.723s | 1.18x
| Clean Product Data    | 1.245s     | 0.982s | 1.27x
| Process Orders | 0.634s| 0.511s | 1.24x

While these improvements are relatively small (18-27%), there are indications that the benefits would be more pronounced with larger datasets.

## Lessons Learned 

1. **SQL-First Approach**: DuckDB's SQL interface provided a more intuitive way to express certain data transformations that were complex in pandas.
2. **Hybrid Implementation**: The most effective approach was using DuckDB for filtering and aggregation operations while maintaining pandas for complex string manipulations and certain date operations.
3. **Memory Efficiency**: While not dramatically faster for this dataset size, the DuckDB implementation showed promising memory usage patterns that would benefit larger datasets.
4. **Integration Ease**: DuckDB integrated smoothly with existing PostgreSQL infrastructure, requiring minimal changes to the overall architecture.

## Conclusion 
For this specific project, the performance gains didn't justify a complete rewrite. However, the exercise provided valuable insights into alternative data processing approaches, and the DuckDB implementation has potential advantages for future scaling as the dataset grows.
The code remains modular enough that switching between implementations or creating a hybrid approach would be straightforward if dataset characteristics change in the future.

