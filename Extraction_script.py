import logging
import airflow
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator


# Define MySQL connection parameters
source_host = 'your host IP'
source_port = 'port'
source_user = 'user'
source_password = 'pass'
source_schema = 'source_database_name'

# Target MySQL connection parameters
target_host = 'target IP'
target_port = 'port'
target_user = 'target_user'
target_password = 'target_pass'
target_schema = 'target_database_name'
target_table = 'target_table'



# Define SQL query for data extraction
extract_query = """
        # Your query logic
"""

# Define function to extract data from source MySQL database
def extract_data():
    logging.info("Extracting data from source MySQL database...")
    source_connection_url = f"mysql+mysqlconnector://{source_user}:{source_password}@{source_host}/{source_schema}"
    engine = create_engine(source_connection_url)
    df = pd.read_sql(extract_query, engine)
    logging.info("Data extraction completed.")
    return df

# Define function to load data into target MySQL database
def load_data(df,target_table_name):
    logging.info("Loading data into target MySQL database...")
    print("Dataframe...")
    print(df)
    target_connection_url = f"mysql+mysqlconnector://{target_user}:{target_password}@{target_host}/{target_schema}"
    engine = create_engine(target_connection_url)
    df.to_sql(target_table_name, engine, if_exists='append', schema=target_schema, index=False)
    logging.info("Data loading completed.")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': False,
    #'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'etl_daily_sales',
    default_args=default_args,
    description='Transfer data from source MySQL database to target MySQL database',
    schedule_interval='@daily',  # Run every hour
    start_date=datetime(2024, 4, 18),
    tags=['mysql', 'data_transfer'],
) as dag:

    # Define task to extract data
    extract_data_from_source = PythonOperator(
        task_id='extract_data_from_source',
        python_callable=extract_data,
    )

    # Define task to load data
    load_data_to_target = PythonOperator(
        task_id='load_data_to_target',
        python_callable=load_data,
        op_kwargs={'df': extract_data_from_source.output, 'target_table_name' : target_table},  # Pass the output of extract_data to load_data
    )

    # Set task dependencies
    extract_data_from_source >> load_data_to_target

# Define logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

