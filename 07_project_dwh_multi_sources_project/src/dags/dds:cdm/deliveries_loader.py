from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from lib.dict_util import str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: int
    order_id: int
    address: str
    delivery_ts: str
    rate: int
    tip_sum: int



class DeliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(self, delivery_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        dd.id, 
                        object_value::json->>'delivery_id' AS delivery_id,
                        dc.id as courier_id,
                        dor.id as order_id, 
                        object_value::json->>'address' AS address, 
                        object_value::json->>'delivery_ts' AS delivery_ts, 
                        object_value::json->>'rate' AS rate, 
                        object_value::json->>'tip_sum' AS tip_sum 
                        FROM stg.delivery_deliveries dd 
                        INNER JOIN dds.dm_couriers dc ON dd.object_value::json->>'courier_id' = dc.courier_id 
                        INNER JOIN dds.dm_orders dor ON dd.object_value::json->>'order_id' = dor.order_key
                    WHERE dd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.   
                    ORDER BY dd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:

    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, courier_id, order_id, address, delivery_ts, rate, tip_sum)
                    VALUES (%(delivery_id)s, %(courier_id)s, %(order_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        order_id = EXCLUDED.order_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "order_id": delivery.order_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                },
            )


class DeliveryLoader:
    WF_KEY = "example_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000 # Рангов мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriesOriginRepository(pg_origin)
        self.stg = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
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
            load_queue = self.origin.list_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")