from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+pg8000://etl_user:Test1234!@localhost:5432/logistics_dw"
)

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1")).scalar()
    print("DB OK:", result)
