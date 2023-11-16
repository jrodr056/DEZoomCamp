from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket


@task(log_prints=True,retries=3)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""
    df = pd.read_csv(dataset_url)
    return df
@task(log_prints=True,retries=3)
def cleanData(df: pd.DataFrame) -> pd.DataFrame:
    """Fix datatype issues"""
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    print(df.head(2))
    print(f"columns:  {df.dtypes}")
    print(f"rows: {len(df)}" )
    return df

@task(log_prints=True,retries=3)
def writeLocal(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as Parquet file"""
    gcsPath = f"data/{color}/{dataset_file}.parquet"
    path = Path(gcsPath)
    df.to_parquet(path,compression="gzip")
    return gcsPath

@task(log_prints=True,retries=3)
def writeGCS(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcpBucket = GcsBucket.load("mygcs")
    print(f"my path is: {path}")
    gcpBucket.upload_from_path(from_path=f"{path}",to_path=path)
    return

@flow()
def etl_web_to_gcs()-> None:
    """The main ETL function"""
    color = "yellow"
    year = "2021"
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    df = fetch(dataset_url)
    dfClean = cleanData(df)
    path = writeLocal(dfClean,color,dataset_file)
    writeGCS(path)
if __name__ == '__main__':
    etl_web_to_gcs()