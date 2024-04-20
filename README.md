## ETL pipeline for data transfer

ETL pipeline to fetch data from a mysql database and load into a target database (mysql), using SQLAlchemy python library and mysql.connector features.
Pipeline is in the form of Airflow DAG utilizing XComs to transfer data between tasks for accurate processing.

### Points:
- Airflow DAG to extract transformed data from mysql database
- Load data from pandas dataframe to a target mysql database
- Libraries such as SQLAlchemy, mysql.connector used for efficient database connection and query execution
- Logging enabled and implemented for error-handling and debugging
- Airflow XCom features used for inter-task communication and data transfer
  
