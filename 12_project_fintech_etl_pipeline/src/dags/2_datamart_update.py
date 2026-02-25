from datetime import timedelta
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.context import Context
import logging

# Конфигурация DAG
default_args = {
    'owner': 'vt251107138d4a',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': True,
}

# Определение функций для расчета и обновления витрины
def calculate_global_metrics(**context):
    execution_date = context['execution_date'].strftime('%Y-%m-%d')
    vertica_hook = VerticaHook(vertica_conn_id='vertica_dwh')
    
    # SQL-запрос для расчета витрины с переводом в единую валюту
    sql = f"""
        INSERT INTO VT251107138D4A__DWH.global_metrics
        SELECT
            '{execution_date}'::DATE AS date_update,
            tr.currency_code AS currency_from,
            SUM(tr.amount * cu.currency_with_div) AS amount_total,  -- Пересчет суммы в единую валюту
            COUNT(*) AS cnt_transactions,
            AVG(tr.amount * cu.currency_with_div) AS avg_transactions_per_account,
            COUNT(DISTINCT tr.account_number_from) AS cnt_accounts_make_transactions
        FROM VT251107138D4A__STAGING.transactions tr
        INNER JOIN VT251107138D4A__STAGING.currencies cu
            ON tr.currency_code = cu.currency_code
            AND tr.transaction_dt::DATE = cu.date_update::DATE
        WHERE tr.transaction_dt::DATE = '{execution_date}'
          AND tr.account_number_from >= 0  -- Исключаем тестовые аккаунты
        GROUP BY tr.currency_code
    """
    
    try:
        vertica_hook.run(sql)
        logging.info(f"Global metrics updated for {execution_date}")
    except Exception as e:
        logging.error(f"Error updating global metrics: {str(e)}")
        raise

# Создание DAG
dag = DAG(
    dag_id='update_global_metrics',
    default_args=default_args,
    description='Update daily global metrics in DWH',
    schedule_interval=timedelta(days=1),
    start_date=pendulum.datetime(2022, 10, 1, tz="UTC"),
    end_date=pendulum.datetime(2022, 10, 31, tz="UTC"),
    catchup=True,
)

# Задача для расчета и обновления витрины
calculate_metrics_task = PythonOperator(
    task_id='calculate_global_metrics',
    python_callable=calculate_global_metrics,
    provide_context=True,
    dag=dag,
)

# Последовательность выполнения задач
calculate_metrics_task