from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from .config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    return create_engine(url, echo=False, future=True)
