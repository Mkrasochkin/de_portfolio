import time
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaProducer, KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
import json
from uuid import UUID

from lib.kafka_connect import KafkaConsumer


class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 cdm_repository: CdmRepository,
                 batch_size: int = 100,
                 logger: Logger = None
                 ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._batch_size = batch_size

    # функция, которая будет вызываться по расписанию.
    def run(self) -> None:
        # Пишем в лог, что джоб был запущен.
        self._logger.info(f"{datetime.now(timezone.utc)}: START")

        # Получите сообщение из Kafka
        i: int = 0
        while i < self._batch_size:
            # Получить сообщение из dds-кафки
            msg = self._consumer.consume()
            if msg is None:
                return
            
            if msg['command_type']=='refresh_mart' and msg['mart_name']=='user_product_counters':
                self._cdm_repository.user_product_counters_refresh()

            if msg['command_type']=='refresh_mart' and msg['mart_name']=='user_category_counters':
                self._cdm_repository.user_category_counters_refresh()

            i+=1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(datetime.now(timezone.utc))}: FINISH")
