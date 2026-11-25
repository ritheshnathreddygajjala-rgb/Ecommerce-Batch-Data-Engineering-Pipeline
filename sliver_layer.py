
from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, to_timestamp
from pyspark.sql.types import StringType


# Reusable function to remove unwanted spaces
def clean_strings(df):

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(field.name, trim(col(field.name)))

    return df


# SILVER ORDERS
@dp.table(name="silver_orders")
def silver_orders():

    df = spark.readStream.table("project1.default.bronze_orders")

    df = clean_strings(df)

    df = (
        df
        .withColumn("order_purchase_timestamp",
                    to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at",
                    to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date",
                    to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date",
                    to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date",
                    to_timestamp("order_estimated_delivery_date"))
    )

    df = df.filter(col("order_id").isNotNull())

    df = df.dropDuplicates(["order_id"])

    return df


# SILVER ORDER ITEMS
@dp.table(name="silver_order_items")
def silver_order_items():

    df = spark.readStream.table("project1.default.bronze_order_items")

    df = clean_strings(df)

    df = (
        df
        .withColumn("order_item_id", col("order_item_id").cast("int"))
        .withColumn("price", col("price").cast("double"))
        .withColumn("freight_value", col("freight_value").cast("double"))
        .withColumn("shipping_limit_date",
                    to_timestamp("shipping_limit_date"))
    )

    df = df.filter(
        col("order_id").isNotNull() &
        col("product_id").isNotNull() &
        col("seller_id").isNotNull()
    )

    df = df.dropDuplicates(["order_id", "order_item_id"])

    return df


# SILVER PRODUCTS
@dp.table(name="silver_products")
def silver_products():

    df = spark.readStream.table("project1.default.bronze_products")

    df = clean_strings(df)

    numeric_columns = [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]

    for column_name in numeric_columns:
        df = df.withColumn(
            column_name,
            col(column_name).cast("double")
        )

    df = df.filter(col("product_id").isNotNull())

    df = df.dropDuplicates(["product_id"])

    return df


# SILVER CUSTOMERS
@dp.table(name="silver_customers")
def silver_customers():

    df = spark.readStream.table("project1.default.bronze_customers")

    df = clean_strings(df)

    df = df.filter(col("customer_id").isNotNull())

    df = df.dropDuplicates(["customer_id"])

    return df