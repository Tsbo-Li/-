from typing import Any, Callable, Optional, TypeVar

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from configs.database_cfg import DatabaseConfig

T = TypeVar("T")


class BaseRepository:
    def __init__(self, connection_uri: Optional[str] = None):
        self.connection_uri = connection_uri or DatabaseConfig.get_pg_uri()
        self.engine: Engine = create_engine(self.connection_uri, echo=False, future=True)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def close(self) -> None:
        self.engine.dispose()

    def run_read(self, callback: Callable[[Session], T]) -> T:
        """
        Read template:
        - Open session
        - Execute callback
        - Close session
        """
        session = self.Session()
        try:
            return callback(session)
        finally:
            session.close()

    def run_write(self, callback: Callable[[Session], T]) -> T:
        """
        Write template:
        - Open session
        - Execute callback
        - Commit on success / rollback on failure
        - Close session
        """
        session = self.Session()
        try:
            result = callback(session)
            session.commit()
            return result
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def read_sql_to_df(self, sql: Any, params: Optional[dict] = None) -> pd.DataFrame:
        """
        Shared SQL->DataFrame template for read repositories.
        """
        return pd.read_sql_query(sql, self.engine, params=params)


def test_base_repository_pipeline() -> None:
    print("=== BaseRepository 自检 ===")
    print(f"[配置URI] {DatabaseConfig.get_pg_uri()}")

    repo = BaseRepository("sqlite+pysqlite:///:memory:")
    print("[1] run_read 演示")
    value = repo.run_read(lambda session: 123)
    print(f"    run_read 返回: {value}")

    print("[2] run_write 演示（成功路径）")
    write_result = repo.run_write(lambda session: "write_ok")
    print(f"    run_write 返回: {write_result}")

    print("[3] read_sql_to_df 演示")
    df = repo.read_sql_to_df(text("SELECT 1 AS value"))
    print(f"    DataFrame输出: {df.to_dict(orient='records')}")
    repo.close()
    print("=== BaseRepository 自检结束 ===")


if __name__ == "__main__":
    test_base_repository_pipeline()
