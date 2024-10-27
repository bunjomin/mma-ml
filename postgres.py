import os
import psycopg2
import sqlalchemy.pool as pool

from dotenv import load_dotenv

load_dotenv()

USER = os.environ.get("POSTGRES_USER", "postgres")
PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
HOST = os.environ.get("POSTGRES_HOST", "localhost")
PORT = int(os.environ.get("POSTGRES_PORT", 5432))
DB = os.environ.get("POSTGRES_DB", "mma")
MAX_OVERFLOW = int(os.environ.get("POOL_MAX_OVERFLOW", 10))
POOL_SIZE = int(os.environ.get("POOL_SIZE", 5))


class Postgres:
    def _get_conn(self):
        return psycopg2.connect(
            user=self._cfg["user"],
            password=self._cfg["password"],
            host=self._cfg["host"],
            port=self._cfg["port"],
            dbname=self._cfg["database"],
        )

    def query(self, query: str, params: tuple | None = None) -> list[tuple] | None:
        returner = None
        conn = self._pool.connect().dbapi_connection
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            returner = cursor.fetchall()
        except Exception as e:
            print("query err", {"query": query, "params": params, "err": e})
        finally:
            cursor.close()
            conn.commit()
        return returner

    def row(self, query: str, params: tuple | None = None) -> dict | tuple | None:
        returner = self.query(query, params)
        if returner and len(returner) > 0:
            returner = returner[0]
        return returner

    def one(self, query: str, params: tuple | None = None) -> any:
        returner = self.row(query, params)
        if not returner:
            return None
        if isinstance(returner, tuple):
            return returner[0]
        if isinstance(returner, dict):
            return returner.get("id")
        return None

    def insert(self, query: str, params: tuple | None = None) -> int | None:
        return self.one(query, params)

    def __init__(self, **kwargs):
        self._cfg = {
            "user": kwargs.get("user", USER),
            "password": kwargs.get("password", PASSWORD),
            "host": kwargs.get("host", HOST),
            "port": kwargs.get("port", PORT),
            "database": kwargs.get("database", DB),
            "max_overflow": kwargs.get("max_overflow", MAX_OVERFLOW),
            "pool_size": kwargs.get("pool_size", POOL_SIZE),
        }
        self._pool = pool.QueuePool(
            self._get_conn,
            max_overflow=self._cfg["max_overflow"],
            pool_size=self._cfg["pool_size"],
        )

    def __del__(self):
        self._pool.dispose()
        self._pool = None
        self._cfg = None
