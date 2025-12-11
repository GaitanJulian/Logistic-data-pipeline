from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from .config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_engine() -> Engine | None:
    # Usamos pg8000 en lugar de psycopg2 para evitar problemas de encoding en Windows
    url = (
        f"postgresql+pg8000://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    )
    # print("DB URL:", url)  # puedes descomentar si quieres ver la URL
    engine = create_engine(url, echo=False, future=True)
    try:
        with engine.connect():
            pass
    except SQLAlchemyError as exc:
        error_message = str(exc).encode("ascii", "backslashreplace").decode("ascii")
        print("DB connection failed:", error_message)
        return None

    return engine
