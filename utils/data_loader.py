from typing import Any, Dict, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from configs.database_cfg import DatabaseConfig


class DataLoader:
    """
    通用数据读取工具:
    - 统一数据库连接
    - 一键读取表到 DataFrame
    - 支持自定义 SQL 查询
    """

    def __init__(self, connection_uri: Optional[str] = None):
        self.connection_uri = connection_uri or DatabaseConfig.get_pg_uri()
        self.engine: Engine = create_engine(self.connection_uri, echo=False, future=True)

    def load_table(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        where_clause: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        读取单表数据并返回 DataFrame。
        示例:
            df = loader.load_table("student_metrics", limit=100)
            df = loader.load_table("student_metrics", where_clause="gpa >= :g", params={"g": 3.0})
        """
        selected_cols = ", ".join(columns) if columns else "*"
        sql = f"SELECT {selected_cols} FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if limit is not None:
            sql += " LIMIT :_limit"
            params = params or {}
            params["_limit"] = int(limit)

        return pd.read_sql_query(text(sql), self.engine, params=params)

    def load_query(self, sql: str, params: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        运行自定义 SQL 并返回 DataFrame。
        示例:
            sql = "SELECT student_id, gpa FROM student_metrics WHERE gpa >= :gpa"
            df = loader.load_query(sql, {"gpa": 3.2})
        """
        return pd.read_sql_query(text(sql), self.engine, params=params)

    def close(self) -> None:
        self.engine.dispose()

    def __enter__(self) -> "DataLoader":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
