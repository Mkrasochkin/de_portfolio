from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaverD:

    def save_object(self, conn: Connection, id: str, delivery_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.delivery_deliveries(object_id, object_value, delivery_ts)
                    VALUES (%(id)s, %(val)s, %(delivery_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        delivery_ts = EXCLUDED.delivery_ts;
                """,
                {
                    "id": id,
                    "val": str_val,
                    "delivery_ts": delivery_ts
                }
            )

