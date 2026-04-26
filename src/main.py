import gc
from config import get_engine
from extract import download_monthly_data, load_trip_data
from setup_schema import run_schema
from transform import (
    clean_trips,
    add_trip_metrics,
    build_datetime_dimension,
    prepare_location_stage,
    build_vendor_dimension,
    clean_fact_nulls,
    apply_business_rules

)
from scd import apply_location_scd
from load import (
    month_already_loaded,
    mark_month_loaded,
    load_datetime_dimension,
    load_vendor_dimension,
    load_fact_table,
    map_surrogate_keys
)


import pandas as pd

def run_pipeline(year: int, month: int):

    print(f"Starting pipeline for {year}-{month:02d}")

    engine = get_engine()
    # 1️⃣ Check metadata
    if month_already_loaded(engine, year, month):
        print("Month already processed. Skipping.")
        return

    # 2️⃣ Download & Load raw data
    file_path = download_monthly_data(year, month)
    df_iter = load_trip_data(file_path, chunksize=1_000_000)

    zones_df = pd.read_csv("taxi_zone_lookup.csv")

    for df in df_iter:
    
        # 1️⃣ Transform
        df = clean_trips(df)
        df = add_trip_metrics(df)
        df = clean_fact_nulls(df)
        df = apply_business_rules(df)
    
        # 2️⃣ Datetime dimension (incremental)
        datetime_dim = build_datetime_dimension(df)
        load_datetime_dimension(datetime_dim, engine)
        # 🔥 ADD THIS HERE (CRITICAL FIX)
        datetime_lookup = pd.read_sql("""
        SELECT datetime_id, full_datetime
        FROM datetime_dim
        """, engine)
    
        # 3️⃣ Location SCD
        location_stage = prepare_location_stage(df, zones_df)
        apply_location_scd(location_stage, engine)
        # (location_lookup can stay outside IF location_dim doesn't change)
        location_lookup = pd.read_sql("""
        SELECT location_sk, location_id
        FROM location_dim
        WHERE is_current = true
        """, engine)
    
        # 4️⃣ Vendor dimension
        vendor_dim = build_vendor_dimension(df)
        load_vendor_dimension(vendor_dim, engine)
    
        # 5️⃣ Map surrogate keys
        df = map_surrogate_keys(df, datetime_lookup, location_lookup)
        assert not df["pickup_datetime_id"].isna().any(), "pickup_datetime_id contains NULLs"
        assert not df["dropoff_datetime_id"].isna().any(), "dropoff_datetime_id contains NULLs"
        assert not df["pickup_location_sk"].isna().any(), "pickup_location_sk contains NULLs"
        assert not df["dropoff_location_sk"].isna().any(), "dropoff_location_sk contains NULLs"

        valid_ratecodes = [1, 2, 3, 4, 5, 6]
        df = df[df["ratecodeid"].isin(valid_ratecodes)]
        # 6️⃣ Load fact table
        load_fact_table(df, engine)
        del df
        gc.collect()


    

    # 9️⃣ Mark metadata
    mark_month_loaded(engine, year, month)

    print(f"Pipeline finished for {year}-{month:02d}")
if __name__ == "__main__":
    run_pipeline(2025, 1)
