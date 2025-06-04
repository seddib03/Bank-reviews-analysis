from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from topic_modeling import perform_lda_analysis
import pandas as pd
from sqlalchemy import create_engine


default_args = {
    'owner': 'admin',
    'email': ['salmaeddib00@gmail.com'],
    'email_on_failure': True,
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'banks_etl_dag',
    default_args=default_args,
    description='Pipeline ETL Bank',
    schedule_interval='@daily',
)

extract_task = BashOperator(
    task_id='scrap',
    bash_command='python3 /home/salma_eddib/airflow/dags/scrape_reviews.py',
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)

loadextract_task = BashOperator(
    task_id='load_reviews',
    bash_command='python3 /home/salma_eddib/airflow/dags/save_to_postgres.py',
    dag=dag,
)
transform_task = BashOperator(
    task_id='dbt_transform',
    bash_command='cd /home/salma_eddib/airflow/bank_reviews_transdbt && dbt run',
    dag=dag,
)
def lda_analysis(**kwargs):
    conn = create_engine('postgresql://airflow_user:airflow_pass@localhost:5432/airflow_db')
    df = pd.read_sql("SELECT * FROM fct_sentiment_analysis", conn)
    topics_df = perform_lda_analysis(df)
    topics_df.to_sql('review_topics', conn, if_exists='replace', index=False)

topic_modeling_task = PythonOperator(
    task_id='topic_modeling',
    python_callable=lda_analysis,
    dag=dag,
)

export_task = BashOperator(
    task_id='export_to_csv',
    bash_command='python3 /home/salma_eddib/airflow/dags/export_csv.py',
    dag=dag,
)


extract_task >> loadextract_task >> transform_task >> topic_modeling_task >> export_task