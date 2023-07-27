from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import requests, pandas as pd
import sqlalchemy
import tweepy
import pandas_gbq

username1= 'root'
password1= 'root'
host= 'host.docker.internal'
db_postgre= 'data_warehouse'
port_postgre = 5433
setting_postgre = f'{username1}:{password1}@{host}:{port_postgre}/{db_postgre}'

def ext(eng_postgre,ti):
    conn_postgre = sqlalchemy.create_engine(f'postgresql+psycopg2://{eng_postgre}')
    df = pd.read_sql(sql='scrap_twitter_from_csv', con=conn_postgre)
    df_xcom = df.to_json(orient = 'records')
    ti.xcom_push(key='raw',value=df_xcom)

def tl(ti):
    data = ti.xcom_pull(key='raw')
    data = pd.read_json(data,orient = 'records')
    data = data[data['tweet_bmkg'].str.contains('^#Gempa')==True]
    data = data['tweet_bmkg'].str.lower()
    df1 = pd.DataFrame({'data':data})
    
    dt = data.str.extract(r'([,]\s[0-9]+[-][a-z]+|[,]\s[0-9]+\s[a-z]+)',expand=True)
    dt = dt[0].str.replace(r',','',regex=True).str.replace(r's','c',regex=True).str.strip()
    dt = dt.str.split(r'-|\s',expand=True)
    
    place = data.str.replace(r'.',' ',regex=True).str.extract(r'(\s[a-z]+[-][a-z]+[)]|\s[a-z]+[)]|[a-z]+[-][a-z]+[)]|\s[a-z]+\s[a-z]+[)])',expand=True)
    place = place[0].str.replace(r')','',regex=True).str.strip()
    place = place.str.replace(r'timurlaut\s|tenggara\s|laut\s|baratlaut\s|selatan\s|kabupaten\s|\sjaya|baratdaya\s|daya\s|kota\s|kab\s|utara\s','',regex=True)
    place = place.str.split('-',n=-1,expand=True)
    place2 = place[0].str.replace(r'\s','',regex=True).str.strip()

    loc = data.str.extract(r'(lok[:].+bt|koordinat[:].+bt)',expand=True)
    loc = loc[0].str.replace(r'(koordinat:|lok:)','',regex=True).str.strip()
    loc = loc.str.split(r'(,|-)',n=-1,expand=True)
    loc = loc.drop([1], axis=1)
    loc[0] = loc[0].apply(lambda x: '-'+x if 'ls' in x else x)
    
    latitude = loc[0].str.replace(r'(ls|lu)','',regex=True).str.strip()
    longitude = loc[2].str.replace(r'(bt)','',regex=True).str.strip()

    data_cleansing = df1[['data']]
    data_cleansing['date'] = pd.to_datetime(((dt[0]+' '+dt[1]+' '+ '2022').str.strip()),format='%d %b %Y')
    data_cleansing['place'] = place2
    data_cleansing['place2'] = place[1]
    data_cleansing['latitude'] = latitude
    data_cleansing['longitude'] = longitude

    data_cleansing.to_csv("/opt/airflow/dags/result_etl.csv",sep=";",index=False)

with DAG(
    dag_id='etl_scrapping_twitter',
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False
) as dag:
    extract = PythonOperator(
        task_id="extract_data_from_postgre",
        python_callable=ext,
        op_kwargs={"eng_postgre": setting_postgre}
    )
    transform_load = PythonOperator(
        task_id='transform_load_data',
        python_callable=tl
    )
    
extract >> transform_load