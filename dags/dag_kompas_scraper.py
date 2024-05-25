#|----------------------------------------------------------------------------------|  
#  Dependecies 
#|----------------------------------------------------------------------------------|

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime,date,timedelta    
import time
from class_scraper import *
from ingestion_tools import *

#|----------------------------------------------------------------------------------|  
#  Functions for task
#|----------------------------------------------------------------------------------| 

#function to extract information from kompas
def connection_test(ti) :

    #create object + run connection_test modul
    scraper_kompass=scraper('https://www.kompas.com/tag/jawa+barat')
    con_result=scraper_kompass.connection_test()
    
    #push to xcom
    ti.xcom_push(key="connection_status", value=con_result)

#function to extract information from kompas
def extract_data_using_scraper(ti) :    
    
    #create object + run scraper
    scraper_kompass=scraper('https://www.kompas.com/tag/jawa+barat')
    scraper_result=scraper_kompass.run_scrapper()

    #push to xcom
    ti.xcom_push(key="scrap_result", value=scraper_result)

    print(type(ti.xcom_pull(key="scrap_result")))

#branching to decide
def split_branch(ti):

    #pull connection_status from xcom
    value=ti.xcom_pull(key='connection_status')
    
    #Logic to determine which branch to take
    if value == 'Connected':
        return 'T4A_run_scraper'
    else:
        return 'T4B_stop_task'

#|----------------------------------------------------------------------------------|  
#  DAG 
#|----------------------------------------------------------------------------------| 

with DAG(
    dag_id='kompas_scraper',
    catchup=True,
    start_date=datetime.now()-timedelta(days=1),
    #start_date=datetime.yesterday(),
    tags=["training"]) as dag :

    #task to create table in the postgres db
    t1_create_tabels=PostgresOperator(
        task_id="T1_create_tabeel_if_not_existt",
        postgres_conn_id='postgres_docker',
        sql='create_tabel.sql',
        dag=dag)

    #task to check connection to kompasa website
    t2_check_connection = PythonOperator(
        task_id='T2_testing_connection',
        python_callable=connection_test,  
        dag=dag)

    #task to check oulled xccom
    t3_branch_task=BranchPythonOperator(
        task_id='T3_split_if_connection_exist_or_not',
        python_callable=split_branch,
        dag=dag)

    #dummy operator
    t4_end_task = DummyOperator(
        task_id='T4B_end_task', 
        dag=dag)

    #task to extract raw value
    t4_run_scrapper = PythonOperator(
        task_id='T4A_run_scraper',
        python_callable=extract_data_using_scraper,
        dag=dag)
    
    #task to insert data raw scraaping result tabel
    t5_insert_raw_result = PythonOperator(
        task_id="T5A_write_to_raw_table",
        python_callable=insert_records,
        dag=dag)

#|----------------------------------------------------------------------------------|  
#  Task dependencies 
#|----------------------------------------------------------------------------------| 

t1_create_tabels >> t2_check_connection >> t3_branch_task
t3_branch_task >> [t4_run_scrapper,t4_end_task]
[t4_run_scrapper] >> t5_insert_raw_result