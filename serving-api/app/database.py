from contextlib import contextmanager
import logging
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from app.config import settings

logger = logging.getLogger("DatabasePool")

connection_pool = pool.ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    host=settings.postgres_host,
    port=settings.postgres_port,
    dbname=settings.postgres_db,
    user=settings.postgres_user,
    password=settings.postgres_password,
)

@contextmanager
def get_db_connection():
    conn = connection_pool.getconn()
    try:
        yield conn
    finally:
        connection_pool.putconn(conn)

@contextmanager
def get_db_cursor():
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            yield cursor