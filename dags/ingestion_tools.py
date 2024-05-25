from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Function to insert records into PostgreSQL
def insert_records(ti):
    pg_hook = PostgresHook(postgres_conn_id='postgres_docker', schema='airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    transformed_data = ti.xcom_pull(key='scrap_result')

    for record in transformed_data:
        record=tuple(record.values())

        cursor.execute(
            "INSERT INTO kompas_scraping_result(scrape_time, story_source, story_news_tag, story_release_date, story_title, story_url, most_common_words) VALUES (%s, %s, %s, %s, %s,%s,%s)",
            record
        )
    connection.commit()
    cursor.close()
    connection.close()