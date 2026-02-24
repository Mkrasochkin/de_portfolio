import time
from datetime import datetime, timezone
from logging import Logger
from lib.kafka_connect.kafka_connectors import KafkaProducer, KafkaConsumer
from dds_loader.repository.dds_repository import DdsRepository
import json

class DdsMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 dds_repository: DdsRepository,
                 batch_size: int = 100,
                 logger: Logger = None
                 ) -> None:
        self._logger = logger
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
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

            # Сохраните сообщение в таблицу.
            msg_json = json.dumps(msg, ensure_ascii=False)
            self._dds_repository.order_update(object_data = msg_json)
            
            # Отправьте выходное сообщения для перестроении витрин
            # user_product_counters
            command_user_product_counters_refresh = {
                "command_type": "refresh_mart",
                "mart_name": "user_product_counters"
            }
            self._producer.produce(command_user_product_counters_refresh)
            
            # user_category_counters
            command_user_category_counters_refresh = {
                "command_type": "refresh_mart",
                "mart_name": "user_category_counters"
            }
            self._producer.produce(command_user_category_counters_refresh)

            i+=1

        # Пишем в лог, что джоб успешно завершен.
        self._logger.info(f"{datetime.now(datetime.now(timezone.utc))}: FINISH")
