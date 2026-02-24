from datetime import datetime
from lib.pg import PgConnect
import json

class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def user_product_counters_refresh(self):
        try:
            with self._db.connection() as conn:
               with conn.cursor() as cur:
                    cur.execute('CALL cdm.user_product_counters_refresh()')
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def user_category_counters_refresh(self):
        try:
            with self._db.connection() as conn:
               with conn.cursor() as cur:
                    cur.execute('CALL cdm.user_category_counters_refresh()')
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()