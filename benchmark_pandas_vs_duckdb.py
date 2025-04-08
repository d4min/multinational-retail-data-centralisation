import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import tabula
import numpy as np

# Import original pandas implementations
from data_cleaning import DataCleaning as PandasDataCleaning
from data_extraction import DataExtractor as PandasDataExtractor

# Import DuckDB implementations
from duckdb_data_cleaning import DataCleaning as DuckDBDataCleaning
from duckdb_data_extraction import DataExtractor as DuckDBDataExtractor

def benchmark_function(func, *args, **kwargs):
    """Measure the execution time of a function"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    execution_time = end_time - start_time
    return execution_time, result

def run_benchmarks():
    """Run benchmarks for comparable methods in both implementations"""
    results = {
        'method': [],
        'pandas_time': [],
        'duckdb_time': [],
        'speedup': []
    }
    
    # initalise extractors and cleaners
    pandas_extractor = PandasDataExtractor()
    duckdb_extractor = DuckDBDataExtractor()
    pandas_cleaner = PandasDataCleaning()
    duckdb_cleaner = DuckDBDataCleaning()
    
    # 1. Benchmark PDF data extraction
    print("Benchmarking PDF data extraction...")
    pdf_link = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    
    pandas_time, pandas_result = benchmark_function(pandas_extractor.retrieve_pdf_data, pdf_link)
    duckdb_time, duckdb_result = benchmark_function(duckdb_extractor.retrieve_pdf_data, pdf_link)
    
    results['method'].append('PDF Data Extraction')
    results['pandas_time'].append(pandas_time)
    results['duckdb_time'].append(duckdb_time)
    results['speedup'].append(pandas_time / duckdb_time if duckdb_time > 0 else float('inf'))
    
    print(f"PDF Data Extraction - Pandas: {pandas_time:.4f}s, DuckDB: {duckdb_time:.4f}s, Speedup: {pandas_time/duckdb_time:.2f}x")
    
    # 2. Benchmark S3 data extraction
    print("Benchmarking S3 data extraction...")
    s3_address = 's3://data-handling-public/products.csv'
    
    try:
        pandas_time, pandas_result = benchmark_function(pandas_extractor.extract_from_s3, s3_address)
        duckdb_time, duckdb_result = benchmark_function(duckdb_extractor.extact_from_s3, s3_address)
        
        results['method'].append('S3 Data Extraction')
        results['pandas_time'].append(pandas_time)
        results['duckdb_time'].append(duckdb_time)
        results['speedup'].append(pandas_time / duckdb_time if duckdb_time > 0 else float('inf'))
        
        print(f"S3 Data Extraction - Pandas: {pandas_time:.4f}s, DuckDB: {duckdb_time:.4f}s, Speedup: {pandas_time/duckdb_time:.2f}x")
        
        # 3. Benchmark Product Weight Conversion
        print("Benchmarking Product Weight Conversion...")
        
        pandas_time, _ = benchmark_function(pandas_cleaner.convert_product_weights, pandas_result.copy())
        duckdb_time, _ = benchmark_function(duckdb_cleaner.convert_product_weights, duckdb_result.copy())
        
        results['method'].append('Product Weight Conversion')
        results['pandas_time'].append(pandas_time)
        results['duckdb_time'].append(duckdb_time)
        results['speedup'].append(pandas_time / duckdb_time if duckdb_time > 0 else float('inf'))
        
        print(f"Product Weight Conversion - Pandas: {pandas_time:.4f}s, DuckDB: {duckdb_time:.4f}s, Speedup: {pandas_time/duckdb_time:.2f}x")
    except Exception as e:
        print(f"Error in S3 extraction or product weight benchmarks: {e}")
    
    # 4. Benchmark API Store Data Retrieval
    print("Benchmarking Store Data API Retrieval...")
    api_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
    
    try:
        pandas_time, _ = benchmark_function(pandas_extractor.retrieve_stores_data, api_endpoint)
        duckdb_time, _ = benchmark_function(duckdb_extractor.retrieve_stores_data, api_endpoint)
        
        results['method'].append('Store Data API Retrieval')
        results['pandas_time'].append(pandas_time)
        results['duckdb_time'].append(duckdb_time)
        results['speedup'].append(pandas_time / duckdb_time if duckdb_time > 0 else float('inf'))
        
        print(f"Store Data API Retrieval - Pandas: {pandas_time:.4f}s, DuckDB: {duckdb_time:.4f}s, Speedup: {pandas_time/duckdb_time:.2f}x")
    except Exception as e:
        print(f"Error in API retrieval benchmark: {e}")
    
    # 5. Benchmark Date Events JSON Processing
    print("Benchmarking Date Events JSON Processing...")
    
    try:
        # For this test, we'll just time the JSON reading and initial processing
        def pandas_json_read():
            df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            df['time_period'] = df['time_period'].astype('category')
            return df
        
        def duckdb_json_read():
            df = pd.read_json('https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json')
            return df
        
        pandas_time, _ = benchmark_function(pandas_json_read)
        duckdb_time, _ = benchmark_function(duckdb_json_read)
        
        results['method'].append('JSON Processing')
        results['pandas_time'].append(pandas_time)
        results['duckdb_time'].append(duckdb_time)
        results['speedup'].append(pandas_time / duckdb_time if duckdb_time > 0 else float('inf'))
        
        print(f"JSON Processing - Pandas: {pandas_time:.4f}s, DuckDB: {duckdb_time:.4f}s, Speedup: {pandas_time/duckdb_time:.2f}x")
    except Exception as e:
        print(f"Error in JSON processing benchmark: {e}")
    
    return results

def create_visualisations(results):
    """Create visualisations comparing the performance of both implementations"""
    # Convert results to a DataFrame
    df = pd.DataFrame(results)
    
    # Set up the figure and style
    plt.figure(figsize=(14, 10))
    sns.set_style("whitegrid")
    
    # 1. Bar chart of execution times
    plt.subplot(2, 1, 1)
    df_melted = pd.melt(df, id_vars=['method'], value_vars=['pandas_time', 'duckdb_time'],
                        var_name='implementation', value_name='execution_time')
    
    # Replace pandas_time and duckdb_time with more readable labels
    df_melted['implementation'] = df_melted['implementation'].replace({
        'pandas_time': 'Pandas',
        'duckdb_time': 'DuckDB'
    })
    
    ax = sns.barplot(x='method', y='execution_time', hue='implementation', data=df_melted)
    plt.title('Execution Time Comparison: Pandas vs DuckDB', fontsize=14)
    plt.xlabel('Method', fontsize=12)
    plt.ylabel('Execution Time (seconds)', fontsize=12)
    plt.xticks(rotation=30, ha='right')
    plt.legend(title='Implementation')
    
    # Add text labels on top of bars
    for i, p in enumerate(ax.patches):
        height = p.get_height()
        ax.text(p.get_x() + p.get_width()/2., height + 0.01,
                f'{height:.3f}s', ha="center", fontsize=9)
    
    # 2. Speedup factor bar chart
    plt.subplot(2, 1, 2)
    speedup_bars = sns.barplot(x='method', y='speedup', data=df, color='green')
    plt.title('DuckDB Speedup over Pandas', fontsize=14)
    plt.xlabel('Method', fontsize=12)
    plt.ylabel('Speedup Factor (higher is better)', fontsize=12)
    plt.xticks(rotation=30, ha='right')
    
    # Add text labels on top of bars
    for i, p in enumerate(speedup_bars.patches):
        height = p.get_height()
        speedup_bars.text(p.get_x() + p.get_width()/2., height + 0.1,
                         f'{height:.2f}x', ha="center", fontsize=9)
    
    plt.tight_layout()
    plt.savefig('pandas_vs_duckdb_benchmarks.png', dpi=300, bbox_inches='tight')
    plt.show()

    # Create a summary table
    print("\nPerformance Summary:")
    print(df[['method', 'pandas_time', 'duckdb_time', 'speedup']].to_string(index=False))
    
    # Save results to CSV for further analysis
    df.to_csv('pandas_vs_duckdb_benchmark_results.csv', index=False)

if __name__ == "__main__":
    print("Starting benchmark of Pandas vs DuckDB implementations...")
    results = run_benchmarks()
    create_visualisations(results)
    print("\nBenchmarking complete! visualisations saved to 'pandas_vs_duckdb_benchmarks.png'")