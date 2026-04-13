import os


class DatabaseConfig:
    DB_HOST = os.getenv("DB_HOST", "localhost")
    DB_PORT = os.getenv("DB_PORT", "5432")
    DB_USER = os.getenv("DB_USER", "xiaozhuyizi")
    DB_PASS = os.getenv("DB_PASS", "123456")
    DB_NAME = os.getenv("DB_NAME", "ideological_profiling_db")

    @classmethod
    def get_pg_uri(cls):
        return f"postgresql://{cls.DB_USER}:{cls.DB_PASS}@{cls.DB_HOST}:{cls.DB_PORT}/{cls.DB_NAME}"


__all__ = ["DatabaseConfig"]
