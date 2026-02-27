import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.delivery_dag.pg_saver_couriers import PgSaver
from examples.stg.delivery_dag.pg_saver_deliveries import PgSaverD

from examples.stg.delivery_dag.couriers_loader import CourierLoader
from examples.stg.delivery_dag.couriers_reader import CourierReader

from examples.stg.delivery_dag.deliveries_loader import DeliveryLoader
from examples.stg.delivery_dag.deliveries_reader import DeliveryReader

from lib import ConnectionBuilder



log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/30 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'api'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_stg_delivery():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
    base_url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    nickname = 'mkrasochkin'
    cohort = '7'
    headers = {
        'X-Nickname': nickname,
        'X-Cohort': cohort,
        'X-API-KEY': api_key
    }

    @task()
    def load_couriers():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = CourierReader(base_url, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()


    @task()
    def load_deliveries():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaverD()

        # Инициализируем класс, реализующий чтение данных из источника.
        collection_reader = DeliveryReader(base_url, headers)

        # Инициализируем класс, в котором реализована бизнес-логика загрузки данных.
        loader = DeliveryLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()


    courier_loader = load_couriers()
    delivery_loader = load_deliveries()


    # Задаем порядок выполнения. Таск только один, поэтому зависимостей нет.
    courier_loader >> delivery_loader# type: ignore


courier_stg_dag = sprint5_project_stg_delivery()  # noqa