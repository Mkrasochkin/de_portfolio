from datetime import datetime, timezone, timedelta
from logging import Logger
from typing import List, Dict

from examples.stg.delivery_dag.pg_saver_deliveries import PgSaverD
from examples.stg.delivery_dag.deliveries_reader import DeliveryReader
from lib import PgConnect
from lib.dict_util import json2str



class DeliveryLoader:
    _LOG_THRESHOLD = 50  
    _SESSION_LIMIT = 50  # Количество записей за один запрос

    def __init__(
        self,
        collection_loader: DeliveryReader,
        pg_dest: PgConnect,
        pg_saver: PgSaverD,
        logger: Logger  
    ) -> None:
        self.collection_loader = collection_loader  
        self.pg_saver = pg_saver  
        self.pg_dest = pg_dest  
        self.log = logger

    def run_copy(self) -> int:
        offset = 0  
        total_loaded = 0

        # Рассчитываем дату 7 дней назад  
        end_date = datetime.now(timezone.utc)
        start_date = end_date - timedelta(days=7)

        self.log.info(f"Loading deliveries from {start_date} to {end_date}")
        

        # Используем сортировку по дате доставки (delivery_ts) по возрастанию  
        sort_field = "delivery_ts"
        sort_direction = "asc"  # или "desc" в зависимости от логики API

        with self.pg_dest.connection() as conn:
            while True:
                # Получаем порцию данных с учетом сортировки и пагинации  
                load_queue = self.collection_loader.get_deliveries(
                    sort_field=sort_field,
                    sort_direction=sort_direction,
                    limit=self._SESSION_LIMIT,
                    offset=offset,
                    start_date=start_date,
                    end_date=end_date  
                )

                if not load_queue:
                    break  # Больше данных нет

                self.log.info(f"Processing batch: {len(load_queue)} records (offset={offset})")

                for delivery in load_queue:
                    self.pg_saver.save_object(
                        conn,
                        str(delivery["order_id"]),
                        delivery["delivery_ts"],
                        delivery  
                    )

                total_loaded += len(load_queue)
                offset += len(load_queue)

                # Логируем прогресс  
                if total_loaded % self._LOG_THRESHOLD == 0:
                    self.log.info(f"Processed {total_loaded} deliveries so far.")

            self.log.info(f"Finished. Total loaded: {total_loaded} deliveries.")
            return total_loaded  
