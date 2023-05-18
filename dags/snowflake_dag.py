from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from models.models import SnowflakeConnector
import pandas as pd
from sql_queries.creating_tables_streams import MASTER_TABLE, RAW_TABLE, RAW_STREAM, \
    STAGE_TABLE, STAGE_STREAM, INSERT_FROM_RAW, INSERT_FROM_STAGE
from dotenv import dotenv_values


'''SET VARIABLES'''
config = dotenv_values("../.env")

SNOWFLAKE_USER = config['SNOWFLAKE_USER']
SNOWFLAKE_PASSWORD = config['SNOWFLAKE_PASSWORD']
SNOWFLAKE_ACCOUNT = config['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = config['SNOWFLAKE_WAREHOUSE']
SNOWFLAKE_DATABASE = config['SNOWFLAKE_DATABASE']

FILE_PATH = config['FILE_PATH']


'''SNOWFLAKE CONNECTOR'''
connector = SnowflakeConnector(user=SNOWFLAKE_USER,
                               password=SNOWFLAKE_PASSWORD,
                               account=SNOWFLAKE_ACCOUNT,
                               warehouse=SNOWFLAKE_WAREHOUSE,
                               database=SNOWFLAKE_DATABASE)


def create_master_table():
    with connector:
        connector.execute_query(MASTER_TABLE)


def create_raw_table():
    with connector:
        connector.execute_query(RAW_TABLE)
        connector.execute_query(RAW_STREAM)


def create_stage_table():
    with connector:
        connector.execute_query(STAGE_TABLE)
        connector.execute_query(STAGE_STREAM)


def file_processing():
    df = pd.read_csv(FILE_PATH, delimiter=',')
    df = df.fillna('-')

    df.loc[df['Total_Average_Rating'] == -1, 'Total_Average_Rating'] = 0
    df.loc[df['Total_Number_of_Ratings'] == -1, 'Total_Number_of_Ratings'] = 0
    df.loc[df['Average_Rating_For_Version'] == -1, 'Average_Rating_For_Version'] = 0
    df.loc[df['Number_of_Ratings_For_Version'] == -1, 'Number_of_Ratings_For_Version'] = 0

    df['Original_Release_Date'] = pd.to_datetime(df['Original_Release_Date'],
                                                 format='%Y-%m-%dT%H:%M:%SZ',
                                                 errors='coerce')
    df['Current_Version_Release_Date'] = pd.to_datetime(df['Current_Version_Release_Date'],
                                                        format='%Y-%m-%dT%H:%M:%SZ',
                                                        errors='coerce')

    df['All_Genres'] = df['All_Genres'].apply(lambda x: str(x))
    df['Languages'] = df['Languages'].apply(lambda x: str(x))

    df['Description'] = df['Description'].astype(str)
    df['Description'] = df['Description'].str.replace('\n', ' ')
    df['description'] = df['Description'].str.replace('|', ' ')

    df.to_csv(FILE_PATH, sep='|', index=False)


def file_upload():
    with connector:
        connector.execute_query('CREATE TEMPORARY STAGE tmp;')
        connector.execute_query(f"PUT '{FILE_PATH}' @tmp/data")
        connector.execute_query("COPY INTO RAW_TABLE "
                                "FROM @tmp/data "
                                "ON_ERROR = 'CONTINUE' "
                                "FILE_FORMAT = (TYPE = CSV, FIELD_DELIMITER='|', error_on_column_count_mismatch=false)")


def insert_from_raw():
    with connector:
        connector.execute_query(INSERT_FROM_RAW)


def insert_from_stage():
    with connector:
        connector.execute_query(INSERT_FROM_STAGE)


with DAG("snowflake_dag",
         start_date=datetime(2023, 5, 11),
         schedule_interval='@daily',
         catchup=False) as dag:

    create_master_table = PythonOperator(
        task_id='create_master_table',
        python_callable=create_master_table,
        do_xcom_push=False
    )

    create_raw_table = PythonOperator(
        task_id='create_raw_table',
        python_callable=create_raw_table,
        do_xcom_push=False
    )

    create_stage_table = PythonOperator(
        task_id='create_stage_table',
        python_callable=create_stage_table,
        do_xcom_push=False
    )

    file_processing = PythonOperator(
        task_id='file_processing',
        python_callable=file_processing,
        do_xcom_push=False
    )

    file_upload = PythonOperator(
        task_id='file_upload',
        python_callable=file_upload,
        do_xcom_push=False
    )

    insert_from_raw = PythonOperator(
        task_id='insert_from_raw',
        python_callable=insert_from_raw,
        do_xcom_push=False
    )

    insert_from_stage = PythonOperator(
        task_id='insert_from_stage',
        python_callable=insert_from_stage,
        do_xcom_push=False
    )

    create_master_table >> [create_raw_table, create_stage_table] \
        >> file_processing >> file_upload >> insert_from_raw >> insert_from_stage
