from config import get_engine
from sqlalchemy import text
import os

def run_schema():

    engine = get_engine()

    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    schema_path = os.path.join(BASE_DIR, "sql", "schema.sql")

    with engine.begin() as conn:
        with open(schema_path, "r") as f:
            schema_sql = f.read()

        conn.execute(text(schema_sql))

    print("Schema created successfully.")

    seed_static_dimensions(engine)

    print("Static dimensions seeded successfully.")


def seed_static_dimensions(engine):

    with engine.begin() as conn:

        conn.execute(text("""
            INSERT INTO payment_dim (payment_type_id, payment_description) VALUES
            (0, 'Flex Fare trip'),
            (1, 'Credit Card'),
            (2, 'Cash'),
            (3, 'No Charge'),
            (4, 'Dispute'),
            (5, 'Unknown'),
            (6, 'Voided trip')
            ON CONFLICT DO NOTHING;
        """))

        conn.execute(text("""
            INSERT INTO ratecode_dim (ratecode_id, rate_description) VALUES
            (1, 'Standard rate'),
            (2, 'JFK'),
            (3, 'Newark'),
            (4, 'Nassau or Westchester'),
            (5, 'Negotiated fare'),
            (6, 'Group ride')
            ON CONFLICT DO NOTHING;
        """))


if __name__ == "__main__":
    run_schema()
