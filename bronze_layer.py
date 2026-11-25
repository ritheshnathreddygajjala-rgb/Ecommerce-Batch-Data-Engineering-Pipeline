
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp, col

source_path = "/Volumes/project1/default/raw_files"

def ingest_csv(file_name):

    raw_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("pathGlobFilter", file_name)
        .load(source_path)
    )

    return (
        raw_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file_name", col("_metadata.file_path"))
          )


@dp.table(name="bronze_orders")
def bronze_orders():
    return ingest_csv("olist_orders_dataset.csv")


@dp.table(name="bronze_order_items")
def bronze_order_items():
    return ingest_csv("olist_order_items_dataset.csv")


@dp.table(name="bronze_products")
def bronze_products():
    return ingest_csv("olist_products_dataset.csv")


@dp.table(name="bronze_customers")
def bronze_customers():
    return ingest_csv("olist_customers_dataset.csv")
