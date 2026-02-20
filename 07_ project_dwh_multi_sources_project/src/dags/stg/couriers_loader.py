from datetime import datetime, timezone, timedelta
from logging import Logger

from examples.stg.delivery_dag.pg_saver_couriers import PgSaver
from examples.stg.delivery_dag.couriers_reader import CourierReader
from lib import PgConnect
from lib.dict_util import json2str
from typing import Optional


class CourierLoader:
    _LOG_THRESHOLD = 50
    _SESSION_LIMIT = 50

    def __init__(self, collection_loader: CourierReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_loader = collection_loader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.log = logger

    def run_copy(self) -> int:
        total_loaded = 0
        offset = 0
        
        # Получаем текущую дату в UTC
        current_date = datetime.now(timezone.utc)
        # Устанавливаем период загрузки - последние 7 дней
        date_from = current_date - timedelta(days=7)

        with self.pg_dest.connection() as conn:
            while True:
                # Получаем порцию данных с учетом offset и limit
                load_queue = self.collection_loader.get_couriers(
                    offset=offset,
                    limit=self._SESSION_LIMIT,
                    sort_field='_id',
                    sort_direction='asc',
                    date_from=date_from,
                    date_to=current_date
                )
                
                self.log.info(f"Found {len(load_queue)} documents to sync from couriers collection (offset={offset}).")
                
                if not load_queue:
                    self.log.info("No more documents to load.")
                    break

                # Сохраняем полученные данные
                i = 0
                for d in load_queue:
                    self.pg_saver.save_object(conn, str(d["_id"]), d)
                    i += 1
                    if i % self._LOG_THRESHOLD == 0:
                        self.log.info(f"Processed {i} documents of current batch.")

                total_loaded += len(load_queue)
                offset += len(load_queue)

                # Если получено меньше запрошенного количества, значит это последняя страница
                if len(load_queue) < self._SESSION_LIMIT:
                    break

            self.log.info(f"Finished loading. Total documents loaded: {total_loaded}")
            return total_loaded


