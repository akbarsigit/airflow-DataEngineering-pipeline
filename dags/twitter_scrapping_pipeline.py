import tweepy
import pandas as pd
import csv
from pathlib import Path
from datetime import timedelta
import psycopg2 as pg
import re

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
csv_path = Path("/opt/airflow/data/tweet_scraper.csv")

default_args = {
    'owner': 'akbar',
    'depends_on_past': False,
    'start_date': days_ago(1),
    # 'email': ['email@mail.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def dataScraping():
    data = []
    query = "covid vaccine OR covid19 vaccine OR coronavirus vaccine"
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth, wait_on_rate_limit=True)

    for tweet in tweepy.Cursor(api.search_tweets, lang='en', tweet_mode="extended", result_type='mixed',q=query, include_entities=True).items(100):
        print(tweet)
        dict_line = {
            "id": str(tweet.id),
            "createdAt": str(tweet.created_at),
            "location": str(tweet.user._json['location'].encode('ascii', 'ignore').decode("ascii")),
            "tweetText": str(tweet.full_text.encode('ascii', 'ignore').decode("ascii"))
        }
        data.append(dict_line)
    return data

def cleaningText(text):
    #Remove Special Character
    text = re.sub(r'[!,*)#%(&$_?.:/"]+<>^', '', text)
    #Remove Tag Account
    text = re.sub(r'@[A-Za-z0-9]+', '', text)
    text = re.sub(r'USERNAME', '', text)
    #Remove Hastag
    text = re.sub(r'#[A-Za-z0-9]+', '', text)
    # Remove retweet
    text = re.sub(r'RT[\s]+', '', text)
    # Remove link
    text = re.sub(r'https?:\/\/\S+', '', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text

def dataTransform(**kwargs):
    # Xcoms to get the data from previous task
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='data_scraping')
    df_tweets = pd.DataFrame(value)
    df_clean_tweets = df_tweets.dropna()
    df_clean_tweets = df_clean_tweets.drop_duplicates('tweetText', keep='first')    

    df_clean_tweets['tweetText'] = df_clean_tweets['tweetText'].apply(cleaningText)

    try:
        if csv_path.exists():
            df_clean_tweets = df_clean_tweets.set_index("id")

            #Drop header
            df_clean_tweets = df_clean_tweets[1:]
            
            df_clean_tweets.to_csv(csv_path, header=False, mode='a')

            # Keep unique data
            df = pd.read_csv(csv_path, sep=",")
            df.drop_duplicates(subset=None, inplace=True)
            df.to_csv(csv_path, index=False)

        else:
            df_clean_tweets = df_clean_tweets.set_index("id")
            df_clean_tweets.to_csv(csv_path, header=True)

        return True
    except OSError as e:
        print(e)
        return False


def dataLoad():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='airflow-postgres-1' password='airflow'"
        )
    except Exception as error:
        print(error)

    #Checking database establishment
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tweet_scraper(
        id varchar(500), 
        createdAt varchar(500),
        location varchar(500),
        tweetText varchar(500));
    """)
    conn.commit()

    with open(csv_path, 'r') as data:
        reader = csv.reader(data)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO tweet_scraper (id, createdAt, location, tweetText) VALUES (%s, %s, %s, %s)",
                row
            )
    conn.commit()

    #Database Checking for duplicate
    cursor.execute("""
    DELETE FROM tweet_scraper a USING (
      SELECT MIN(ctid) as ctid, tweetText
        FROM tweet_scraper 
        GROUP BY tweetText HAVING COUNT(*) > 1
      ) b
      WHERE a.tweetText = b.tweetText 
      AND a.ctid <> b.ctid
    """)
    conn.commit()

dag = DAG(
    dag_id = 'twitter_scraping_pipeline',
    default_args=default_args,
    description='twitter scraping DAG',
    # schedule_interval=timedelta(days=1),
    schedule_interval='@once',	
)

scrapping = PythonOperator(
    task_id='data_scraping',
    python_callable=dataScraping,
    dag=dag)

transforming = PythonOperator(
    task_id='data_transform',
    python_callable=dataTransform,
    provide_context=True,
    dag=dag)

saving = PythonOperator(
    task_id='data_load',
    python_callable=dataLoad,
    dag=dag)

scrapping >>  transforming >> saving