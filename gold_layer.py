
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, count, year, month, date_format
)

@dp.table(name="gold_monthly_orders")
def gold_monthly_orders():

    orders_df = spark.readStream.table(
        "project1.default.silver_orders"
    )

    result = (
        orders_df
        .withColumn(
            "order_year",
            year(col("order_purchase_timestamp"))
        )
        .withColumn(
            "order_month",
            month(col("order_purchase_timestamp"))
        )
        .groupBy("order_year", "order_month")
        .agg(
            count("order_id").alias("total_orders")
        )
        .orderBy("order_year", "order_month")
    )

    return result



from pyspark import pipelines as dp
from pyspark.sql.functions import sum, count, col

@dp.materialized_view(
    name="gold_product_performance_mv"
)
def gold_product_performance():

    # Read Silver tables as batch DataFrames
    items_df = spark.read.table(
        "project1.default.silver_order_items"
    )

    products_df = spark.read.table(
        "project1.default.silver_products"
    )

    # Join both datasets
    joined_df = items_df.join(
        products_df,
        on="product_id",
        how="inner"
    )

    # Calculate product performance
    result = (
        joined_df
        .groupBy(
            "product_id",
            "product_category_name"
        )
        .agg(
            count("*").alias("total_items_sold"),
            sum("price").alias("total_revenue")
        )
    )

    return result


from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, year, month

@dp.materialized_view(
    name="gold_monthly_orders_mv"
)
def gold_monthly_orders():

    orders_df = spark.read.table(
        "project1.default.silver_orders"
    )

    result = (
        orders_df
        .withColumn(
            "order_year",
            year(col("order_purchase_timestamp"))
        )
        .withColumn(
            "order_month",
            month(col("order_purchase_timestamp"))
        )
        .groupBy(
            "order_year",
            "order_month"
        )
        .agg(
            count("order_id").alias("total_orders")
        )
    )

    return result



from pyspark import pipelines as dp
from pyspark.sql.functions import col, count

@dp.materialized_view(
    name="gold_customer_analysis_mv"
)
def gold_customer_analysis():

    orders_df = spark.read.table(
        "project1.default.silver_orders"
    )

    customers_df = spark.read.table(
        "project1.default.silver_customers"
    )

    # Join orders and customers
    joined_df = orders_df.join(
        customers_df,
        on="customer_id",
        how="inner"
    )

    # Calculate customer metrics
    result = (
        joined_df
        .groupBy(
            "customer_id",
            "customer_city",
            "customer_state"
        )
        .agg(
            count("order_id").alias("total_orders")
        )
    )

    return result


from pyspark import pipelines as dp
from pyspark.sql.functions import count

@dp.materialized_view(name="gold_order_status_mv")
def gold_order_status():

    orders_df = spark.read.table("project1.default.silver_orders")

    result = (
        orders_df
        .groupBy("order_status")
        .agg(
            count("order_id").alias("total_orders")
        )
    )

    return result


from pyspark import pipelines as dp
from pyspark.sql.functions import (
    count, col, sum, when, countDistinct
)

@dp.materialized_view(name="gold_data_quality_mv")
def gold_data_quality():

    orders_df = spark.read.table(
        "project1.default.silver_orders"
    )

    result = orders_df.agg(
        count("*").alias("total_records"),

        sum(
            when(col("order_id").isNull(), 1).otherwise(0)
        ).alias("missing_order_ids"),

        (count("order_id") - countDistinct("order_id"))
        .alias("duplicate_order_ids"),

        sum(
            when(col("order_status").isNull(), 1).otherwise(0)
        ).alias("missing_order_status")
    )

    return result


from pyspark import pipelines as dp
from pyspark.sql.functions import col, count, sum, when

@dp.materialized_view(name="gold_order_items_quality_mv")
def gold_order_items_quality():

    items_df = spark.read.table(
        "project1.default.silver_order_items"
    )

    result = items_df.agg(
        count("*").alias("total_records"),

        sum(
            when(col("order_id").isNull(), 1).otherwise(0)
        ).alias("missing_order_ids"),

        sum(
            when(col("product_id").isNull(), 1).otherwise(0)
        ).alias("missing_product_ids"),

        sum(
            when(col("price").isNull() | (col("price") <= 0), 1)
            .otherwise(0)
        ).alias("invalid_prices"),

        sum(
            when(col("freight_value").isNull() | (col("freight_value") < 0), 1)
            .otherwise(0)
        ).alias("invalid_freight_values")
    )

    return result


from pyspark import pipelines as dp
from pyspark.sql.functions import col, lit

@dp.materialized_view(name="gold_consolidated_quality_mv")
def gold_consolidated_quality():

    orders_quality = spark.read.table(
        "project1.default.gold_data_quality_mv"
    )

    items_quality = spark.read.table(
        "project1.default.gold_order_items_quality_mv"
    )

    orders_report = orders_quality.select(
        lit("Orders").alias("table_name"),
        col("total_records"),
        col("missing_order_ids").alias("missing_ids"),
        col("duplicate_order_ids").alias("duplicate_ids")
    )

    items_report = items_quality.select(
        lit("Order Items").alias("table_name"),
        col("total_records"),
        col("missing_order_ids").alias("missing_ids"),
        lit(None).cast("long").alias("duplicate_ids")
    )

    return orders_report.unionByName(items_report)





