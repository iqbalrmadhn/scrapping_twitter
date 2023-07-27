from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests, pandas as pd
import sqlalchemy
from datetime import datetime
import os
import logging

username1= 'root'
password1= 'root'
host= 'host.docker.internal'
db_postgre= 'data_warehouse'
port_postgre = 5433
setting_postgre = f'{username1}:{password1}@{host}:{port_postgre}/{db_postgre}'
logger = logging.getLogger(__name__)

def read_file(ti):
    ls_dir=[]
    root=os.getcwd()
    os.chdir("/opt/airflow/dags/")
    for path, subdirs, files in os.walk(root):
        for name in files:
            ls_dir.append(os.path.join(path, name))
    ls_dir=[i for i in ls_dir if i.__contains__("scraping")]
    logger.info(ls_dir)
    for i,file in enumerate(ls_dir):
        df = pd.read_csv(file, delimiter=";", index_col=False)
        if i==0:
            df_union = df
        else:
            df_union = df_union.append(df,ignore_index=True)
    df_xcom = df_union.to_json(orient = 'records')
    ti.xcom_push(key='dt_union',value=df_xcom)

def migrate(eng_postgre,ti):
    data = ti.xcom_pull(key='dt_union')
    data = pd.read_json(data,orient = 'records')
    conn_postgre = sqlalchemy.create_engine(f'postgresql+psycopg2://{eng_postgre}')
    data.to_sql(con=conn_postgre, name='scrap_twitter_from_csv', if_exists='replace', index=False)

with DAG(
    dag_id='ingest_from_csv',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    read_data = PythonOperator(
        task_id='read_data',
        python_callable=read_file,
    )
    insert_to_db = PythonOperator(
        task_id='insert_to_db',
        python_callable=migrate,
        op_kwargs={"eng_postgre": setting_postgre
        }
    )
read_data >> insert_to_db