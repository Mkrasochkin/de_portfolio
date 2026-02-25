from airflow import DAG  
from airflow.operators.python import PythonOperator  
from airflow.hooks.postgres_hook import PostgresHook  
from airflow.providers.vertica.hooks.vertica import VerticaHook  
from datetime import datetime, timedelta  
import pandas as pd  
import logging

default_args = {
    'owner': 'vt251107138d4a',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
}

def extract_data(**context):  
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    pg_hook = PostgresHook(postgres_conn_id='pgsql_prod')
    
    transactions_query = f"""
        SELECT 
            operation_id,
            account_number_from,
            account_number_to,
            currency_code,
            country,
            status,
            transaction_type,
            amount,
            transaction_dt::text as transaction_dt_str  
        FROM public.transactions  
        WHERE DATE(transaction_dt) = '{execution_date}'
        AND status = 'done'
    """
     
    currencies_query = f"""
        SELECT 
            date_update::text as date_update_str,
            currency_code,
            currency_code_with,
            currency_with_div  
        FROM public.currencies  
        WHERE DATE(date_update) = '{execution_date}'
    """
    
    try:
        transactions_df = pg_hook.get_pandas_df(transactions_query)
        currencies_df = pg_hook.get_pandas_df(currencies_query)
        
        context['ti'].xcom_push(key='transactions', value=transactions_df.to_json(orient='split'))
        context['ti'].xcom_push(key='currencies', value=currencies_df.to_json(orient='split'))
        
        logging.info(f"Successfully extracted data for {execution_date}")
    except Exception as e:
        logging.error(f"Error extracting data: {str(e)}")
        raise

def load_data(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    vertica_hook = VerticaHook(vertica_conn_id='vertica_dwh')
    
    transactions_json = context['ti'].xcom_pull(key='transactions')
    currencies_json = context['ti'].xcom_pull(key='currencies')
    
    transactions_df = pd.read_json(transactions_json, orient='split')
    currencies_df = pd.read_json(currencies_json, orient='split')
    
    conn = vertica_hook.get_conn()
    cursor = conn.cursor()
    
    try:
        # Загрузка транзакций  
        if not transactions_df.empty:
            insert_transactions = """
                INSERT INTO VT251107138D4A__STAGING.transactions (
                    operation_id, account_number_from, account_number_to,
                    currency_code, country, status, transaction_type,
                    amount, transaction_dt  
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # Конвертация строк обратно в datetime для Vertica  
            transactions_df['transaction_dt'] = pd.to_datetime(transactions_df['transaction_dt_str'])
            cursor.executemany(insert_transactions, transactions_df[
                ['operation_id', 'account_number_from', 'account_number_to',
                 'currency_code', 'country', 'status', 'transaction_type',
                 'amount', 'transaction_dt']
            ].values.tolist())
        
        # Загрузка курсов валют  
        if not currencies_df.empty:
            insert_currencies = """
                INSERT INTO VT251107138D4A__STAGING.currencies (
                    date_update, currency_code, currency_code_with, currency_with_div  
                ) VALUES (%s, %s, %s, %s)
            """
            # Конвертация строк обратно в datetime для Vertica  
            currencies_df['date_update'] = pd.to_datetime(currencies_df['date_update_str'])
            cursor.executemany(insert_currencies, currencies_df[
                ['date_update', 'currency_code', 'currency_code_with', 'currency_with_div']
            ].values.tolist())
        
        conn.commit()
        logging.info(f"Successfully loaded data for {execution_date}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading data: {str(e)}")
        raise  
    finally:
        cursor.close()
        conn.close()

with DAG(
    'vertica_etl_pipeline_fixed',
    default_args=default_args,
    description='Fixed ETL pipeline for loading data from PG to Vertica',
    schedule_interval='0 1 * * *',
    start_date=datetime(2022, 10, 1),
    end_date=datetime(2022, 10, 31),
    catchup=True,
    max_active_runs=1,
    tags=['data_engineering'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> load_task


