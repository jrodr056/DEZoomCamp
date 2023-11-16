python 06_spark_sql.py \
    --input_green=data/raw/green/2022/*/ \
    --input_yellow=data/raw/yellow/2022/*/ \
    --output=data/reports-2022

URL="spark://192.168.50.212:7077"
spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
    --input_green=data/raw/green/2022/*/ \
    --input_yellow=data/raw/yellow/2022/*/ \
    --output=data/reports-2022


    --input_green=gs://jrod-de-zoomcamp-bucket/pq/green/2022/*/ \
    --input_yellow=gs://jrod-de-zoomcamp-bucket/pq/yellow/2022/*/ \
    --output=trips_data_all.reports-2022

    jrod-de-zoomcamp-bucket/pq
    
tripsDataAll.reports-2022