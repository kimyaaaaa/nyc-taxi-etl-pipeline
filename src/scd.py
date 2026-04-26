from sqlalchemy import text
from datetime import datetime
import pandas as pd
from sqlalchemy import text

def apply_location_scd(location_stage: pd.DataFrame, engine):
    """
    Applies SCD Type 2 logic to location_dim.
    """

    now = datetime.utcnow()

    with engine.begin() as conn:

        for _, row in location_stage.iterrows():

            # Fetch current record for this natural key
            existing = pd.read_sql(
                text("""
                SELECT *
                FROM location_dim
                WHERE location_id = :id
                  AND is_current = true
            """),
            conn,
            params={"id": row["location_id"]})

            # CASE 1 — New location (not found in dimension)
            if existing.empty:

                conn.execute(text("""
                    INSERT INTO location_dim
                    (location_id, borough, zone, service_zone,
                     valid_from, valid_to, is_current)
                    VALUES (:id, :b, :z, :s, :from, NULL, true)
                """), {
                    "id": row["location_id"],
                    "b": row["borough"],
                    "z": row["zone"],
                    "s": row["service_zone"],
                    "from": now
                })

            else:
                old = existing.iloc[0]

                # Check if descriptive attributes changed
                attributes_changed = (
                        old["borough"] != row["borough"] or
                        old["zone"] != row["zone"] or
                        old["service_zone"] != row["service_zone"]
                )

                # CASE 2 — No change → do nothing
                if not attributes_changed:
                    continue

                # CASE 3 — Change detected
                # Expire old record
                conn.execute(text("""
                    UPDATE location_dim
                    SET is_current = false,
                        valid_to = :now
                    WHERE location_sk = :sk
                """), {
                    "now": now,
                    "sk": old["location_sk"]
                })

                # Insert new version
                conn.execute(text("""
                    INSERT INTO location_dim
                    (location_id, borough, zone, service_zone,
                     valid_from, valid_to, is_current)
                    VALUES (:id, :b, :z, :s, :from, NULL, true)
                """), {
                    "id": row["location_id"],
                    "b": row["borough"],
                    "z": row["zone"],
                    "s": row["service_zone"],
                    "from": now
                })
