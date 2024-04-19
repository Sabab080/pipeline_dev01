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
source_host = '157.230.43.223'
source_port = '3306'
source_user = 'noor'
source_password = 'OkOGf0iIOnwK56hd'
source_schema = 'zingpay'

# Target MySQL connection parameters
target_host = '127.0.0.1'
target_port = 3306
target_user = 'test'
target_password = 'test1234'
target_schema = 'zingpay_target'
target_table = 'daily_sales_table'



# Define SQL query for data extraction
extract_query = """
    Select current_date() as 'insertion_date', 
	date(transaction.datetime) 'date_value',
    app_user.full_name 'customer_name',
    app_user.username 'username',
    app_user.business_name 'business_name',
    app_user.parent_id 'parent_id',
    (select concat(full_name,"-",username) from app_user au2 where account_id = app_user.parent_id) 'parent_name_id', 
    organization_branch.branch_name 'branch_name',
    zingpay_transaction_type.description 'transaction_type_id',
    ifnull(sum(( select transaction.amount where transaction_type_id='1')), 0) as debit,
    ifnull(sum(( select transaction.amount where transaction_type_id='2')),0) as credit,
    Count(( select transaction.amount where transaction_type_id='1')) as debit_count,
    count(( select transaction.amount where transaction_type_id='2')) as credit_count,
    ifnull(sum(( select transaction.amount where transaction_type_id='2')),0) - ifnull(sum(( select transaction.amount where transaction_type_id='1')), 0) as 'net'    
    from zingpay.transaction 
    left join zingpay.app_user on app_user.account_id=transaction.account_id
    left join zingpay.zingpay_transaction_type on zingpay_transaction_type.zingpay_transaction_type_id=transaction.zingpay_transaction_type_id
    left join transaction_status on transaction_status.id=transaction.transaction_status_id
    left join organization_branch on app_user.branch_id=organization_branch.branch_id
    where  transaction.transaction_status_id=1
        and date(datetime)>=current_date()-100
    Group by 
    date(transaction.datetime),
    app_user.full_name,
    app_user.username,
    app_user.business_name,
    app_user.parent_id,
    (select concat(full_name,"-",username) from app_user au2 where account_id = app_user.parent_id),
    organization_branch.branch_name,
    zingpay_transaction_type.description;
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

