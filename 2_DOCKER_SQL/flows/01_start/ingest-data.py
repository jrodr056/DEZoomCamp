import pandas as pd
from time import time
import sys
import os
import argparse
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

@task(log_prints=True,retries=3,cache_key_fn=task_input_hash,cache_expiration=timedelta(days=1))
def extract_data(url):
    parquetName = "output.parquet"
    csvName = "output.csv"
    os.system(f"wget {url} -O {parquetName}")
    pd.read_parquet(parquetName).to_csv(csvName,index=False)

    df_iter = pd.read_csv(csvName,iterator=True,chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True,retries=3)
def transformData(df):
    print(f"pre:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count']!=0]
    print(f"post:missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True,retries=3)
def ingest(tableName,tData):
    sqlConnect = SqlAlchemyConnector.load("mypostgrecon")
    with sqlConnect.get_connection(begin=False) as engine:
        tData.head(n=0).to_sql(name=tableName,con=engine,if_exists="replace")
        tData.to_sql(name=tableName,con=engine,if_exists="append")

# @flow(name="Subflow",log_prints=True)
# def log_subflow()
@flow(name="Ingest Flow")
def main_flow():
    tableName = "yellow_taxi_trips"
    url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    rawData = extract_data(url)
    tData = transformData(rawData)
    ingest(tableName,tData)

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