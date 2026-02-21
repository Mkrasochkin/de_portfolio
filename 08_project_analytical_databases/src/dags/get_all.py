from airflow import DAG
from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.operators.vertica import VerticaOperator
from datetime import datetime
import boto3
import vertica_python

# Конфигурация
AWS_ACCESS_KEY_ID = "YOUR_ACCESS_KEY"
AWS_SECRET_ACCESS_KEY = "YOUR_SECRET_KEY"
BUCKET_NAME = 'sprint6'
FILES_TO_DOWNLOAD = ['users.csv', 'groups.csv', 'dialogs.csv', 'group_log.csv']
DOWNLOAD_PATH = '/data/'
SCHEMA_NAME = 'STV2025061617__STAGING'

# Параметры подключения к Vertica
VERTICA_CONN = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv2025061617',
    'password': 'iBP68LOFfXLdyet',
    'database': 'dwh',
    'autocommit': True
}

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'{DOWNLOAD_PATH}{key}'
    )

def load_data_to_vertica(table_name: str):
    conn = vertica_python.connect(**VERTICA_CONN)
    cursor = conn.cursor()
    
    try:
        # Вариант 1: Только таблица отклоненных записей
        cursor.execute(f"""
            COPY {SCHEMA_NAME}.{table_name} 
            FROM LOCAL '{DOWNLOAD_PATH}{table_name}.csv' 
            DELIMITER ',' 
            SKIP 1
            REJECTED DATA AS TABLE {SCHEMA_NAME}.{table_name}_rej
        """) 
    except Exception as e:
        print(f"Error loading {table_name}: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

@dag(
    schedule_interval=None,
    start_date=datetime(2022, 7, 13),
    catchup=False,
    tags=['s3', 'vertica', 'staging']
)
def sprint6_dag_load_staging():
    # Задачи для скачивания файлов
    download_tasks = []
    for filename in FILES_TO_DOWNLOAD:
        task = PythonOperator(
            task_id=f'download_{filename}',
            python_callable=fetch_s3_file,
            op_kwargs={
                'bucket': BUCKET_NAME,
                'key': filename
            },
        )
        download_tasks.append(task)
    
    # Задачи для загрузки данных в Vertica
    load_users = PythonOperator(
        task_id='load_users',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'users'}
    )
 
    load_groups = PythonOperator(
        task_id='load_groups',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'groups'}
    )
 
    load_dialogs = PythonOperator(
        task_id='load_dialogs',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'dialogs'}
    )

    load_group_log = PythonOperator(
        task_id='load_group_log',
        python_callable=load_data_to_vertica,
        op_kwargs={'table_name': 'group_log'}
    )
  
    # Определяем зависимости между задачами
    download_tasks >> load_users >>  load_groups >>  load_dialogs >> load_group_log


# Создаем DAG
sprint6_dag = sprint6_dag_load_staging()
