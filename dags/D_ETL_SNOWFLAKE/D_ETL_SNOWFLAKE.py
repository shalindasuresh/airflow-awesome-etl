import logging
import shutil
import time
from pprint import pprint
import requests
import csv
from bs4 import BeautifulSoup


import pendulum

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

with DAG(
    dag_id='D_ETL_SNOWFLAKE',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 8, 5, tz="UTC"),
    catchup=True,
    tags=['etl','web_scrape','sf_load'],
) as dag:


# TASK LIST FOR DAG
    @task(task_id="scrape_bda")
    def scrape_bda(ds=None, **kwargs):


        orig_url = 'https://bigdataanalyst.blog/wp-json/wp/v2/posts/'

        headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        wp_posts = requests.get(orig_url, headers=headers).json()

        post_titles=[]
        for post in wp_posts:
            post_title = post['title']['rendered']
            post_content = post['content']['rendered']
            
            post_titles.append([post_title])    
            
            
        header = ['post_title']

        with open('/opt/bitnami/airflow/dags/D_ETL_SNOWFLAKE/posts.csv', 'w', encoding='UTF8', newline='') as f:
            writer = csv.writer(f)

            # write the header
            writer.writerow(header)

            # write the data
            writer.writerows(post_titles)
        
        
        return 'Scraped Succesfully'


    @task(task_id="load_snowflake")
    def load_snowflake(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        pprint(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    scrape = scrape_bda()
    load = load_snowflake()

    scrape>>load
