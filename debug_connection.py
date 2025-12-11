from sqlalchemy import text
from etl.db import get_engine

def main():
    engine = get_engine()

    print("Connecting using URL:", engine.url)

    with engine.connect() as conn:
        print("\n=== Connection info ===")
        db, user = conn.execute(
            text("SELECT current_database(), current_user")
        ).fetchone()
        print("Database:", db)
        print("User:", user)

        print("\n=== Tables found in schema 'public' ===")
        rows = conn.execute(
            text("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
        ).fetchall()

        for (table,) in rows:
            print("-", table)

if __name__ == "__main__":
    main()
