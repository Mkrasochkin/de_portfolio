from logging import Logger
from typing import List
from datetime import datetime, timedelta, timezone


from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    user_id: int
    restaurant_id: int
    timestamp_id: int
    delivery_id: int
    

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                    WITH order_data AS (
                    SELECT
                        id,
                        object_id as order_key,
                        object_value::json->>'final_status' as order_status,
                        (object_value::json->'restaurant'->>'id') as restaurant_id_str,
                        (object_value::json->>'date')::timestamp as timestamp,
                        (object_value::json->'user'->>'id') as user_id_str
                    FROM stg.ordersystem_orders
                    WHERE object_value::json->>'final_status' IN ('CLOSED', 'CANCELLED')
                    ORDER BY id ASC
                ),
                delivery_data AS (
                    SELECT
                        od.id,
                        od.order_key,
                        od.order_status,
                        od.restaurant_id_str,
                        od.timestamp,
                        od.user_id_str,
                        object_value::json->>'delivery_id' as delivery_id_str
                    FROM stg.delivery_deliveries gd
                    INNER JOIN order_data od ON od.order_key = (gd.object_value::json->>'order_id')
                    ORDER BY id ASC
                )
                SELECT
                    dd.id,
                    dd.order_key,
                    dd.order_status,
                    u.id as user_id,
                    r.id as restaurant_id,
                    t.id as timestamp_id,
                    COALESCE(d.id, 0) as delivery_id
                FROM delivery_data dd
                INNER JOIN dds.dm_restaurants r ON dd.restaurant_id_str = r.restaurant_id
                INNER JOIN dds.dm_timestamps t ON dd.timestamp = t.ts
                INNER JOIN dds.dm_users u ON dd.user_id_str = u.user_id
                INNER JOIN dds.dm_deliveries d ON dd.delivery_id_str = d.delivery_id
                WHERE dd.id > %(threshold)s
                ORDER BY dd.id ASC
                LIMIT %(limit)s 
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, user_id, restaurant_id, timestamp_id, delivery_id)
                    VALUES (%(order_key)s, %(order_status)s, %(user_id)s, %(restaurant_id)s, %(timestamp_id)s, %(delivery_id)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_key = EXCLUDED.order_key,
                        order_status = EXCLUDED.order_status,
                        user_id = EXCLUDED.user_id,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        delivery_id = EXCLUDED.delivery_id;
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "user_id": order.user_id,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "delivery_id": order.delivery_id
                },
            )


class OrderLoader:
    WF_KEY = "example_orders_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 10000 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_origin)
        self.stg = OrderDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")