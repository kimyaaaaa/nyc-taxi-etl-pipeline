from sqlalchemy import text
from datetime import datetime
import pandas as pd

def month_already_loaded(engine, year: int, month: int) -> bool:
    """
    Checks if given year and month already processed.
    """

    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT 1
            FROM etl_metadata
            WHERE year = :year
              AND month = :month
        """), {"year": year, "month": month})

        return result.fetchone() is not None
def mark_month_loaded(engine, year: int, month: int):
    """
    Marks month as processed.
    """

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO etl_metadata (year, month, loaded_at)
            VALUES (:year, :month, :loaded_at)
        """), {
            "year": year,
            "month": month,
            "loaded_at": datetime.utcnow()
        })
def load_datetime_dimension(datetime_df: pd.DataFrame, engine):
    """
    Inserts new datetime records only.
    """

    existing = pd.read_sql(
        "SELECT full_datetime FROM datetime_dim",
        engine
    )

    new_rows = datetime_df[
        ~datetime_df["full_datetime"].isin(existing["full_datetime"])
    ]

    if not new_rows.empty:
        new_rows.to_sql(
            "datetime_dim",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
def load_vendor_dimension(vendor_df: pd.DataFrame, engine):

    existing = pd.read_sql(
        "SELECT vendor_id FROM vendor_dim",
        engine
    )

    new_rows = vendor_df[
        ~vendor_df["vendor_id"].isin(existing["vendor_id"])
    ]

    if not new_rows.empty:
        new_rows.to_sql(
            "vendor_dim",
            engine,
            if_exists="append",
            index=False
        )
def map_surrogate_keys(df: pd.DataFrame, datetime_lookup, location_lookup) -> pd.DataFrame:
    """
    Maps natural keys to surrogate keys for fact table.
    """

    # =============================
    # 1️⃣ Map datetime surrogate keys
    # =============================

    # Pickup datetime mapping
    df = df.merge(
        datetime_lookup,
        left_on="tpep_pickup_datetime",
        right_on="full_datetime",
        how="left"
    ).rename(columns={"datetime_id": "pickup_datetime_id"}) \
        .drop(columns=["full_datetime"])

    # Dropoff datetime mapping
    df = df.merge(
        datetime_lookup,
        left_on="tpep_dropoff_datetime",
        right_on="full_datetime",
        how="left"
    ).rename(columns={"datetime_id": "dropoff_datetime_id"}) \
        .drop(columns=["full_datetime"])

    # =============================
    # 2️⃣ Map location surrogate keys
    # =============================

    # Pickup location
    df = df.merge(
        location_lookup,
        left_on="pulocationid",
        right_on="location_id",
        how="left"
    ).rename(columns={"location_sk": "pickup_location_sk"}) \
        .drop(columns=["location_id"])

    # Dropoff location
    df = df.merge(
        location_lookup,
        left_on="dolocationid",
        right_on="location_id",
        how="left"
    ).rename(columns={"location_sk": "dropoff_location_sk"}) \
        .drop(columns=["location_id"])

    # =============================
    # 3️⃣ Drop natural keys
    # =============================

    df = df.drop(columns=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pulocationid",
        "dolocationid"
    ])

    return df

def load_fact_table(fact_df: pd.DataFrame, engine):
    fact_df = fact_df.rename(columns={
    "vendorid": "vendor_id",
    "ratecodeid": "ratecode_id",
    "payment_type": "payment_type_id"
    })
    fact_df.to_sql(
        "trips_fact",
        engine,
        if_exists="append",
        index=False,
        chunksize=30000
    )
