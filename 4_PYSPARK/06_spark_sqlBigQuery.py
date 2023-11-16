import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
import argparse


parser = argparse.ArgumentParser()
parser.add_argument('--input_green',required=True)
parser.add_argument('--input_yellow',required=True)
parser.add_argument('--output',required=True)

args = parser.parse_args()
input_green = args.input_green
input_yellow = args.input_yellow
output = args.output

spark = SparkSession.builder \
    .appName("test") \
    .getOrCreate()

spark.conf.set('tempGcsBucket', 'dataproc-temp-us-west2-242870064208-amhgnjbp')
df_green = spark.read.parquet(input_green)


df_green.printSchema()

df_yellow = spark.read.parquet(input_yellow)

df_green.createOrReplaceTempView('green')
df_yellow.createOrReplaceTempView('yellow')

df_green = df_green.withColumnRenamed('lpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('lpep_dropoff_datetime','dropoff_datetime')

df_yellow = df_yellow.withColumnRenamed('tpep_pickup_datetime','pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime','dropoff_datetime')


commonCols = []
yellowCols = set(df_yellow.columns)
for col in df_green.columns:
    if col in yellowCols:
        commonCols.append(col)

greenSelect = df_green.select(commonCols).withColumn('service_type',F.lit('green'))
yellowSelect = df_yellow.select(commonCols).withColumn('service_type',F.lit('yellow'))

dfTripsData = greenSelect.unionAll(yellowSelect)

dfTripsData.groupBy('service_type').count().show()


dfTripsData.createOrReplaceTempView('tripsData')

spark.sql("""SELECT service_type, count(service_type) FROM tripsData group by service_type;""").show()

dfYellowRev = spark.sql("""
SELECT date_trunc('hour',tpep_pickup_datetime) as hour, PULocationID as zone, sum(total_amount) as amount, count(1) as number_records
from yellow where tpep_pickup_datetime >= '2022-01-01 00:00:00'
group by hour, zone;""")

dfGreenRev = spark.sql("""
SELECT date_trunc('hour',lpep_pickup_datetime) as hour, PULocationID as zone, sum(total_amount) as amount, count(1) as number_records
from green where lpep_pickup_datetime >= '2022-01-01 00:00:00'
group by hour, zone;""")

df_greenT = dfGreenRev.withColumnRenamed('amount','green_amount').withColumnRenamed('number_records','green_records')
df_yellowT = dfYellowRev.withColumnRenamed('amount','yellow_amount').withColumnRenamed('number_records','yellow_records')

dfYellowGreen = df_yellowT.join(df_greenT,on=['hour','zone'],how='outer')

dfYellowGreen.show()

dfYellowGreen.write.format('bigquery') \
    .option('temporaryGcsBucket','bucket').save(output)
