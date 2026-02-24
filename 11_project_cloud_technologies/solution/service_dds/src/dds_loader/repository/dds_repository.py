import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel
import json

class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def order_update(self, object_data: json):
        try:
            with self._db.connection() as conn:
               with conn.cursor() as cur:
                    cur.execute('CALL dds.load_order_from_json(%s)', [object_data])
        except Exception as e:
            print(e)
            conn.rollback()
            raise e
        finally:
            conn.close()
