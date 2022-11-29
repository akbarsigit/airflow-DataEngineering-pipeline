import tweepy
import pandas as pd
import csv
from pathlib import Path
from datetime import timedelta
import glob
import psycopg2 as pg

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


consumer_key = 'FosPbDO1qjdUy4lJIpHGqRu6J'
consumer_secret = 'u50oLz5zvrUBfmykzRiGBzgPDTxf7dwTTJFMoTDbRAoUWV1ANT'
access_key = '875758575959670784-TPp0E4sBIsSx7uxzzJVCkZt0eHE7jTE'
access_secret = 'pzZLCss26ZYFSZTaWjRyWGu54TXSk8dGlV8d11AvQ7GKE'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'twitter_crawl2',
    default_args=default_args,
    description='twitter crawling DAG',
    schedule_interval=timedelta(days=1),
)


def get_auth():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api


def get_data_search(**kwargs):
    data = []
    query = "#WorldCup"
    api = get_auth()
    cursor = tweepy.Cursor(api.search_tweets, result_type='mixed',
                           q=query, include_entities=True).items(100)
    c = 0
    for x in cursor:
        c = c + 1
        print(x)
        data.append(x)
        if c == 100:
            break
    return data


def parse_data(**context):
    all_data = []
    value = context['task_instance'].xcom_pull(task_ids='crawling_data')
    for tweet in value:
        print(tweet)
        dict_line = {
            "twitId": str(tweet.id),
            "twitContent": str(tweet.text.encode('ascii', 'ignore').decode("ascii"))
        }
        all_data.append(dict_line)
    return all_data


csv_path = Path("/opt/airflow/data/tweets_crawl_v2.csv")


def save_data(**kwargs):
    # Xcoms to get the list
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='parsing_data')
    df = pd.DataFrame(value)
    df1 = df.dropna()

    try:
        print(df1)
        df1.to_csv(csv_path, index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False


def addtodb():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='airflow-postgres-1' password='airflow'"
        )
    except Exception as error:
        print(error)

    path = "/opt/airflow/data/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
        fname = fname.split('/')
        csvname = fname[-1]
        csvname = csvname.split('.')
        tablename = str(csvname[0])

    # create the table if it does not already exist
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS """ + tablename + """ (
                twitId varchar(100),
                twitContent varchar(350)
            );
        """
                       )
        conn.commit()

    # insert each csv row as a record in our database
    with open('/opt/airflow/data/tweets_crawl_v2.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO tweets_tweets_crawl_v2 VALUES (%s, %s, %s)",
                row
            )
    conn.commit()


t1 = PythonOperator(
    task_id='crawling_data',
    python_callable=get_data_search,
    # provide_context=True,
    dag=dag)

t2 = PythonOperator(
    task_id='parsing_data',
    python_callable=parse_data,
    provide_context=True,
    dag=dag)
t3 = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    provide_context=True,
    dag=dag)
t4 = PythonOperator(
    task_id='add_db',
    python_callable=addtodb,
    dag=dag)

t1 >> t2 >> t3 >> t4