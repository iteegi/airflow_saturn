import os

from datetime import datetime

import requests
import logging

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.apache.hive.hooks.hive import HiveCliHook
from airflow.providers.apache.hive.operators.hive import HiveOperator

BASE_ENDPOINT = 'CSSEGISandData/COVID-19/tree/master/csse_covid_19_data/csse_covid_19_daily_reports/'
HIVE_TABLE = 'COVID_RESULTS'


def check_if_table_exists(**kwargs):
    table = kwargs['table'].lower()
    conn = HiveCliHook(hive_cli_conn_id=kwargs['conn_id'])
    logging.info(f'Checking if hive table {table} exists.')
    query = f"show tables in default like '{table}';"
    logging.info(f'Running query: {query}')
    results = conn.run_cli(hql=query)
    if 'OK' in results:
        if table in results:
            logging.info(
                f'Table {table} exists. Proceed with adding new partition.')
            return 'load_to_hive'
        else:
            logging.info(
                f'Table {table} does not exists. Proceed with hive table creation.')
            return 'create_hive_table'
    else:
        raise RuntimeError(
            f'Hive returned not OK status while running query: {query}')


def download_covid_data(**kwargs):
    conn = BaseHook.get_connection(kwargs['conn_id'])
    url = conn.host + kwargs['endpoint'] + kwargs['exec_date'] + '.csv'
    logging.info(f'Sending get request to COVID-19 repository by url: {url}')
    response = requests.get(url)

    if response.status_code == 200:
        save_path = f"/opt/airflow/dags/files/{kwargs['exec_date']}.csv"
        logging.info(f'Successfully get data. Saving to file: {save_path}')
        with open(save_path) as f:
            f.write(response.text)
    else:
        raise ValueError(f'Unable get data url: {url}')


default_arg = {
    'owner': 'chel',
    'email': 'chel@gmail.com',
    'email_on_retry': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': 60,
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'end_date': datetime(2023, 3, 8),
}

with DAG(
    dag_id='covid_daily_data',
    tags=['daily', 'covid-19'],
    description='Daily upload of COVID-19 data',
    schedule_interval='0 7 * * *',
    max_active_runs=2,
    concurrency=4,
    default_args=default_arg,
    user_defined_macros={
        'convert_date': lambda dt: dt.strtime('%m-%d-%Y')
    }
) as main_dag:

    EXEC_DATE = '{{convert_date(execution_date)}}'

    check_if_data_avaible = HttpSensor(
        task_id='check_if_data_avaible',
        http_conn_id='covid_api',
        endpoint=f'{BASE_ENDPOINT}{EXEC_DATE}.csv',
        poke_interval=60,
        timeout=600,
        soft_fail=False,
        mode='reschedule',
    )

    download_data = PythonOperator(
        task_id='download_data',
        python_callable=download_covid_data,
        op_kwargs={
            'conn_id': 'covid_api',
            'endpoint': BASE_ENDPOINT,
            'exec_date': EXEC_DATE,
        }
    )

    move_data_to_hdfs = BashOperator(
        task_id='move_data_to_hdfs',
        bash_command="""
            hdfs dfs -mkdir -p /covid_data/csv
            hdfs dfs -copyFromLocal /opt/airflow/dags/files/{exec_date}.csv /covid_data/csv
            rm /opt/airflow/dags/files/{exec_date}.csv
        """.format(exec_date=EXEC_DATE)
    )

    process_data = SparkSubmitOperator(
        task_id='process_data',
        application=os.path.join(
            main_dag.folder, 'scripts/covid_data_processing.py'),
        conn_id='spark_conn',
        name=f'{main_dag.dag_id}_process_data',
        application_args=[
            '--exec_date', EXEC_DATE
        ]
    )

    check_if_hive_table_exists = BranchPythonOperator(
        task_id='check_if_hive_table_exists',
        python_callable=check_if_table_exists,
        op_kwargs={
            'table': HIVE_TABLE,
            'conn_id': 'hive_conn',
        }
    )

    create_hive_table = HiveOperator(
        task_id='create_hive_table',
        hive_cli_conn_id='hive_conn',
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS default.{table}(
                country_region STRING,
                total_confirmed INT,
                total_deaths INT,
                fatality_ratio DOUBLE,
                world_case_pct DOUBLE,
                world_death_pct DOUBLE
            )
            PARTITIONED BY (exec_date STRING)
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
            LOCATION '/covid_data/results';
        """.format(table=HIVE_TABLE)
    )

    load_to_hive = HiveOperator(
        task_id='load_to_hive',
        hive_cli_conn_id='hive_conn',
        hql=f"MSCK REPAIR TABLE default.{HIVE_TABLE};",
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    check_if_data_avaible >> download_data >> move_data_to_hdfs >> process_data >> check_if_hive_table_exists
    check_if_hive_table_exists >> [create_hive_table, load_to_hive]
    create_hive_table >> load_to_hive
