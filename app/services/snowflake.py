# app/services/snowflake.py
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import snowflake.connector
from snowflake.connector import DictCursor

from app.config import settings

logger = logging.getLogger(__name__)


class SnowflakeService:
    def __init__(self) -> None:
        self._conn = None

    def connect(self):
        if self._conn is None or self._conn.is_closed():
            self._conn = snowflake.connector.connect(
                account=settings.snowflake_account,
                user=settings.snowflake_user,
                password=settings.snowflake_password,
                warehouse=settings.snowflake_warehouse,
                database=settings.snowflake_database,
                schema=settings.snowflake_schema,
                role=settings.snowflake_role,
            )
        return self._conn

    def close(self) -> None:
        try:
            if self._conn is not None:
                self._conn.close()
        finally:
            self._conn = None

    def execute_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        conn = self.connect()
        with conn.cursor(DictCursor) as cur:
            cur.execute(sql, params or {})
            return [dict(r) for r in cur.fetchall()]

    def execute_update(self, sql: str, params: Optional[Dict[str, Any]] = None) -> None:
        conn = self.connect()
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
        conn.commit()
