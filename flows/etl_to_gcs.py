from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(retries=3)
def fetch(dataset_file: str) -> pd.DataFrame: # return dataframe
    """ Read taxi data from web into DataFrame"""
    df = pd.read_csv(dataset_file)

    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_name: str) -> Path:
    """Write DataFrame out as parquet file"""
    path = Path(f"data/{color}/{dataset_name}.parquet")
    df.to_parquet(path, compression="gzip")

    return path

@task()
def write_gcs(path: Path, color: str, dataset_name: str) -> None:
    """Upload parquet file to GCS"""
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        # vì path ở Window là \ mà trên GCS là / nên ta phải specify path url 
        to_path=f"data/{color}/{dataset_name}.parquet"     
    )

@flow()
def etl_to_gcs() -> None:  # mean not return 
    """ Main ETL """
    color = "yellow"
    year = 2021
    month = 1
    dataset_name = f"{color}_tripdata_{year}-{month:02}.csv.gz"
    dataset_file = f"./data/{color}/{dataset_name}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/{color}/{dataset_file}.csv.gz"
    
    df = fetch(dataset_file)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_name)
    write_gcs(path, color, dataset_name)


if __name__ == '__main__':
    etl_to_gcs()

