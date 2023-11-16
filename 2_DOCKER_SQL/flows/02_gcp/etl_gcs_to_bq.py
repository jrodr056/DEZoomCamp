from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True,retries=3)
def extract_from_gcs(color: str, year: int, month:int) -> Path:
    """Download trip data from GCS"""
    gcsPath = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    localPath = Path(f"data\{color}")
    gcsBlock = GcsBucket.load("mygcs")
    gcsBlock.get_directory(from_path=gcsPath,local_path="")
    return localPath

@task(log_prints=True,retries=3)
def transformData(path: Path) -> pd.DataFrame:
    """Data cleaning sample"""
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0,inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True,retries=3)
def writeBQ(df: pd.DataFrame) -> None:
    """Write DF to Biq Query"""
    GcpCreds = GcpCredentials.load("mygcpcreds")
    df.to_gbq(destination_table="mydataset.rides",project_id="focus-vim-397017",credentials=GcpCreds.get_credentials_from_service_account(),chunksize=500,if_exists="append")


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Biq Query"""
    color="yellow"
    year=2021
    month=1

    path = extract_from_gcs(color,year,month)
    df = transformData(path)
    writeBQ(df)

if __name__ == "__main__":
    etl_gcs_to_bq()