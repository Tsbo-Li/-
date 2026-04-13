from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from configs.database_cfg import DatabaseConfig


class BaseRepository:
    def __init__(self, connection_uri: Optional[str] = None):
        self.connection_uri = connection_uri or DatabaseConfig.get_pg_uri()
        self.engine: Engine = create_engine(self.connection_uri, echo=False, future=True)
        self.Session = sessionmaker(bind=self.engine, autoflush=False, autocommit=False)

    def close(self) -> None:
        self.engine.dispose()
