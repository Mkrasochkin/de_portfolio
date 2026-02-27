import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.couriers_loader import CourierLoader
from examples.dds.deliveries_loader import DeliveryLoader
from examples.dds.courier_ledger_loader import CourierLedgerLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/1 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'project'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_delivery_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Создаем подключение к базе подсистемы бонусов.
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        # создаем экземпляр класса, в котором реализована логика.
        cour_loader = CourierLoader(origin_pg_connect, dwh_pg_connect, log)
        cour_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    # Объявляем таск, который загружает данные.
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        # создаем экземпляр класса, в котором реализована логика.
        deliv_loader = DeliveryLoader(origin_pg_connect, dwh_pg_connect, log)
        deliv_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.


    # Объявляем таск, который загружает данные.
    @task(task_id="dm_courier_ledger_load")
    def load_dm_courier_ledger():
        # создаем экземпляр класса, в котором реализована логика.
        deliv_loader = CourierLedgerLoader(origin_pg_connect, dwh_pg_connect, log)
        deliv_loader.load_dm_courier_ledger()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные таски.
    couriers_dict = load_dm_couriers()
    deliveries_dict = load_dm_deliveries()
    courier_ledger_dict = load_dm_courier_ledger()
    
    

    # Далее задаем последовательность выполнения тасков.
    couriers_dict >> deliveries_dict >> courier_ledger_dict


delivery_dag = sprint5_example_delivery_dag()