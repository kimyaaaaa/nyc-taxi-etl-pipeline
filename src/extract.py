import os
import requests
import pandas as pd
from config import BASE_URL, RAW_PATH
import pyarrow.parquet as pq

def download_monthly_data(year: int, month: int) -> str:
    """
    Downloads yellow taxi data for a specific year and month.
    Returns local file path.
    """

    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{file_name}"

    os.makedirs(RAW_PATH, exist_ok=True)
    local_path = os.path.join(RAW_PATH, file_name)

    # Avoid re-downloading if file already exists
    if not os.path.exists(local_path):
        print(f"Downloading {file_name}...")

        response = requests.get(url)
        response.raise_for_status()

        with open(local_path, "wb") as f:
            f.write(response.content)

        print("Download complete.")

    else:
        print(f"{file_name} already exists. Skipping download.")

    return local_path



def load_trip_data(file_path: str, chunksize: int = 50000):
    """
    Proper streaming of parquet file using PyArrow.
    """

    parquet_file = pq.ParquetFile(file_path)

    print(f"Total rows in dataset: {parquet_file.metadata.num_rows}")

    for i, batch in enumerate(parquet_file.iter_batches(batch_size=chunksize)):
        df = batch.to_pandas()
        df.columns = df.columns.str.lower()

        start = i * chunksize
        end = start + len(df)

        print(f"Processing rows {start} → {end}")

        yield df