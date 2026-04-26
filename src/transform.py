import pandas as pd


def clean_trips(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes invalid trips:
    - Negative or zero distance
    - Dropoff before pickup
    """

    df = df[
        (df["trip_distance"] > 0) &
        (df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"])
        ]

    return df.reset_index(drop=True)
def add_trip_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds derived metrics:
    - trip_duration_minutes
    - avg_speed_mph
    - is_tipped
    """

    df["trip_duration_minutes"] = (
            (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
            .dt.total_seconds() / 60
    )

    df["avg_speed_mph"] = (
            df["trip_distance"] /
            (df["trip_duration_minutes"] / 60)
    )

    df["avg_speed_mph"] = df["avg_speed_mph"].clip(0, 120)

    df["is_tipped"] = df["tip_amount"] > 0

    return df


def get_time_of_day(hour: int) -> str:
    if 5 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"
def build_datetime_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates datetime dimension dataframe.
    """

    all_datetimes = pd.concat([
        df["tpep_pickup_datetime"],
        df["tpep_dropoff_datetime"]
    ]).drop_duplicates().sort_values()

    datetime_dim = pd.DataFrame({
        "full_datetime": all_datetimes
    }).reset_index(drop=True)

    datetime_dim["minute"] = datetime_dim["full_datetime"].dt.minute
    datetime_dim["hour"] = datetime_dim["full_datetime"].dt.hour
    datetime_dim["day"] = datetime_dim["full_datetime"].dt.day
    datetime_dim["month"] = datetime_dim["full_datetime"].dt.month
    datetime_dim["year"] = datetime_dim["full_datetime"].dt.year
    datetime_dim["weekday"] = datetime_dim["full_datetime"].dt.weekday
    datetime_dim["weekday_name"] = datetime_dim["full_datetime"].dt.day_name()
    datetime_dim["is_weekend"] = datetime_dim["weekday"].isin([5, 6])
    datetime_dim["time_of_day"] = datetime_dim["hour"].apply(get_time_of_day)

    return datetime_dim
def prepare_location_stage(df: pd.DataFrame, zones_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepares location staging dataframe for SCD processing.
    """

    pu = df[["pulocationid"]].rename(columns={"pulocationid": "location_id"})
    do = df[["dolocationid"]].rename(columns={"dolocationid": "location_id"})

    locations = pd.concat([pu, do]).drop_duplicates()

    zones_df = zones_df.rename(columns={"locationid": "location_id"})
    zones_df.columns = zones_df.columns.str.lower()

    location_stage = locations.merge(
        zones_df,
        on="location_id",
        how="left"
    )

    location_stage[["borough", "zone", "service_zone"]] = (
        location_stage[["borough", "zone", "service_zone"]]
        .fillna("Unknown")
    )

    return location_stage
def build_vendor_dimension(df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds vendor dimension from trip data.
    """

    vendor_dim = (
        df[["vendorid"]]
        .drop_duplicates()
        .rename(columns={"vendorid": "vendor_id"})
    )

    # Map known vendor IDs to names
    vendor_names = {
        1: "Creative Mobile Technologies",
        2: "VeriFone"
    }

    vendor_dim["vendor_name"] = (
        vendor_dim["vendor_id"]
        .map(vendor_names)
        .fillna("Unknown")
    )

    return vendor_dim
def clean_fact_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans nullable fact columns before loading.
    """

    df["passenger_count"] = df["passenger_count"].fillna(1)
    df["congestion_surcharge"] = df["congestion_surcharge"].fillna(0)
    df["airport_fee"] = df["airport_fee"].fillna(0)
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna("N")

    return df
def apply_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies business validation rules to fact data.
    """

    # Distance & money caps
    df = df[
        (df["trip_distance"] <= 1000) &
        (df["fare_amount"] <= 10000) &
        (df["total_amount"] <= 10000)
        ]

    # Duration limits
    df = df[
        (df["trip_duration_minutes"] >= 1) &
        (df["trip_duration_minutes"] <= 300)
        ]

    # No negative money values
    money_cols = ["fare_amount","total_amount","tip_amount","tolls_amount","extra"]

    for c in money_cols:
        df = df[df[c] >= 0]

    # Recalculate and clip speed
    df["avg_speed_mph"] = (
            df["trip_distance"] /
            (df["trip_duration_minutes"] / 60)
    )

    df["avg_speed_mph"] = df["avg_speed_mph"].clip(0, 120)

    return df
