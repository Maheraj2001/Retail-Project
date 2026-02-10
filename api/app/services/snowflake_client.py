import os
import snowflake.connector
from typing import Optional


class SnowflakeClient:
    """Wrapper around Snowflake connector for query execution."""

    def __init__(self):
        self.conn = None

    def connect(self):
        self.conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "DATALAKEHOUSE"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "GOLD"),
            role=os.environ.get("SNOWFLAKE_ROLE", "DATA_READER"),
        )

    def close(self):
        if self.conn:
            self.conn.close()

    def fetch_all(self, query: str, params: Optional[dict] = None) -> list[dict]:
        cursor = self.conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(query, params or {})
        return cursor.fetchall()

    def fetch_one(self, query: str, params: Optional[dict] = None) -> dict:
        cursor = self.conn.cursor(snowflake.connector.DictCursor)
        cursor.execute(query, params or {})
        return cursor.fetchone()
