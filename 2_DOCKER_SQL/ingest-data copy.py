import pandas as pd
from time import time
import sys
import os
import argparse
from sqlalchemy import create_engine
from prefect import flow, task


@task(log_prints=True,retries=3)
def ingest():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    tableName = "yellow_taxi_trips"
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"

    # user = params.user
    # password = params.password
    # host = params.host
    # port = params.port
    # db = params.db
    # tableName = params.tableName
    # url = params.url
    parquetName = "output.parquet"
    csvName = "output.csv"
    #download the csv
    os.system(f"wget {url} -O {parquetName}")
    pd.read_parquet(parquetName).to_csv(csvName,index=False)

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csvName,iterator=True,chunksize=100000)

    df = next(df_iter)

    # df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=tableName,con=engine,if_exists="replace")
    df.to_sql(name=tableName,con=engine,if_exists="append")


@flow(name="Ingest Flow")
def main_flow():
    ingest()
if __name__ == "__main__":

    # parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # parser.add_argument('--user', help='user name for postgres')
    # parser.add_argument('--password', help='password for postgres')
    # parser.add_argument('--host', help='host name for postgres')
    # parser.add_argument('--port', help='port name for postgres')
    # parser.add_argument('--db', help='db name for postgres')
    # parser.add_argument('--tableName', help='table name for postgres')
    # parser.add_argument('--url', help='url name for postgres')
    
    # parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    # parser.add_argument('--user', help='user name for postgres')
    # parser.add_argument('--password', help='password for postgres')
    # parser.add_argument('--host', help='host name for postgres')
    # parser.add_argument('--port', help='port name for postgres')
    # parser.add_argument('--db', help='db name for postgres')
    # parser.add_argument('--tableName', help='table name for postgres')
    # parser.add_argument('--url', help='url name for postgres')

    # args = parser.parse_args()
    # ingest(args)

    main_flow()