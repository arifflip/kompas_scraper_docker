#|----------------------------------------------------------------------------------|  
#  Dependecies 
#|----------------------------------------------------------------------------------|

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime,date,timedelta
import time
from class_scraper2 import *

#|----------------------------------------------------------------------------------|  
#  Functions for task
#|----------------------------------------------------------------------------------| 

#function to extract information from kompas
def extract_data_using_scraper() :
    
    scraper_kompass=scraper('https://www.kompas.com/tag/jawa+barat')
    result=scraper_kompass.run_scrapper()

    return result

#def ingestion_to_raw_table() :



#|----------------------------------------------------------------------------------|  
#  DAG 
#|----------------------------------------------------------------------------------| 

with DAG(
    dag_id='kompas_scraper',
    catchup=True,
    start_date=datetime.now()-timedelta(days=1),
    #start_date=datetime.yesterday(),
    tags=["training"]) as dag :

    #task to check connection to kompasa website
    t1_check_connection = PythonOperator(
        task_id='testing_connection',
        python_callable=connection_test,  
        dag=dag,
    )

    #task to pull from
#    t2_run_scraper = PythonOperator(
#        task_id='extract_data_using_scraper',
#        python_callable=extract_data_using_scraper,
#        dag=dag)

    t2_create_tabels=PostgresOperator(
        task_id="create_tabeel_if_not_existt",
        postgres_conn_id='postgres_docker',
        sql='create_tabel.sql',
        dag=dag)

#    t1_check_connection >> t2_create_tabels >> t2_run_scraper
t1_check_connection >> t2_create_tabels