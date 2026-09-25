import argparse
import json
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


def parse_event(line):
    try:
        return json.loads(line)
    except Exception:
        return None


def to_product_metric(event):
    product_id = event.get("product_id")
    category = event.get("category") or "unknown"
    event_type = event.get("event_type")

    clicks = 1 if event_type == "click" else 0
    carts = 1 if event_type == "add_to_cart" else 0
    purchases = 1 if event_type == "purchase" else 0

    quantity = 0
    revenue = 0.0

    if event_type == "purchase":
        quantity = int(event.get("quantity") or 0)
        unit_price = float(event.get("unit_price") or 0.0)
        revenue = quantity * unit_price

    return (
        (product_id, category),
        (
            clicks,
            carts,
            purchases,
            quantity,
            revenue,
        ),
    )


def sum_metrics(left, right):
    return (
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
        left[3] + right[3],
        left[4] + right[4],
    )


def main():
    parser = argparse.ArgumentParser()

    default_date = (
        datetime.now(timezone.utc).date()
        - timedelta(days=1)
    ).isoformat()

    parser.add_argument(
        "--date",
        default=default_date,
        help="Data histórica no formato YYYY-MM-DD",
    )

    args = parser.parse_args()

    year, month, day = args.date.split("-")

    input_path = (
        "hdfs://namenode:9000/"
        f"bigdata/ecommerce/raw/{year}/{month}/{day}/*/*.jsonl"
    )

    output_path = (
        "hdfs://namenode:9000/"
        "bigdata/ecommerce/curated/daily_product_metrics"
    )

    spark = (
        SparkSession.builder
        .appName("Ecommerce Historical ETL")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.warehouse.dir",
            "hdfs://namenode:9000/"
            "bigdata/ecommerce/hive/warehouse"
        )
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    sc = spark.sparkContext

    print(f"[spark] data de referencia: {args.date}")
    print(f"[spark] lendo: {input_path}")

    # ---------------------------------------------------------
    # RDD
    # ---------------------------------------------------------

    raw_rdd = sc.textFile(input_path)

    events_rdd = (
        raw_rdd
        .map(parse_event)
        .filter(lambda event: event is not None)
    )

    product_events_rdd = (
        events_rdd
        .filter(
            lambda event:
                event.get("product_id") is not None
        )
        .cache()
    )

    total_events = events_rdd.count()
    product_events = product_events_rdd.count()

    print(
        f"[spark] eventos validos: {total_events}"
    )

    print(
        f"[spark] eventos de produto: {product_events}"
    )

    # ---------------------------------------------------------
    # WIDE DEPENDENCY
    #
    # reduceByKey exige shuffle entre particoes.
    # Portanto esta operacao representa explicitamente
    # uma dependencia ampla (wide dependency).
    # ---------------------------------------------------------

    metrics_rdd = (
        product_events_rdd
        .map(to_product_metric)
        .reduceByKey(sum_metrics)
    )

    print(
        "[spark] wide dependency executada: reduceByKey"
    )

    print("[spark] lineage do RDD:")
    print(metrics_rdd.toDebugString())

    rows_rdd = metrics_rdd.map(
        lambda item: (
            item[0][0],       # product_id
            item[0][1],       # category
            item[1][0],       # clicks
            item[1][1],       # add_to_cart
            item[1][2],       # purchases
            item[1][3],       # units_sold
            float(item[1][4]) # revenue
        )
    )

    metrics_df = spark.createDataFrame(
        rows_rdd,
        [
            "product_id",
            "category",
            "clicks",
            "add_to_cart",
            "purchases",
            "units_sold",
            "revenue",
        ],
    )

    # ---------------------------------------------------------
    # Spark SQL
    # ---------------------------------------------------------

    metrics_df.createOrReplaceTempView(
        "product_metrics_stage"
    )

    result_df = spark.sql(
        f"""
        SELECT
            product_id,
            category,
            clicks,
            add_to_cart,
            purchases,
            units_sold,
            CAST(
                ROUND(revenue, 2)
                AS DOUBLE
            ) AS revenue,

            CAST(
                CASE
                    WHEN clicks > 0
                    THEN ROUND(
                        purchases * 100.0 / clicks,
                        2
                    )
                    ELSE 0.0
                END
                AS DOUBLE
            ) AS conversion_rate_pct,

            '{args.date}' AS reference_date

        FROM product_metrics_stage

        ORDER BY
            revenue DESC,
            product_id
        """
    )

    print("[spark] schema do consolidado:")
    result_df.printSchema()

    print("[spark] consolidado diario:")

    result_df.show(
        50,
        truncate=False
    )

    # Preserva outras particoes caso processemos
    # mais de um dia no futuro.
    spark.conf.set(
        "spark.sql.sources.partitionOverwriteMode",
        "dynamic"
    )

    (
        result_df
        .write
        .mode("overwrite")
        .partitionBy("reference_date")
        .parquet(output_path)
    )

    print(
        f"[spark] resultado gravado em: {output_path}"
    )

    # ---------------------------------------------------------
    # Hive Data Warehouse
    # ---------------------------------------------------------

    spark.sql(
        """
        CREATE DATABASE IF NOT EXISTS ecommerce
        LOCATION
        'hdfs://namenode:9000/bigdata/ecommerce/hive/ecommerce.db'
        """
    )

    spark.sql(
        """
        CREATE EXTERNAL TABLE IF NOT EXISTS
        ecommerce.daily_product_metrics (
            product_id STRING,
            category STRING,
            clicks BIGINT,
            add_to_cart BIGINT,
            purchases BIGINT,
            units_sold BIGINT,
            revenue DOUBLE,
            conversion_rate_pct DOUBLE
        )
        PARTITIONED BY (
            reference_date STRING
        )
        STORED AS PARQUET
        LOCATION
        'hdfs://namenode:9000/bigdata/ecommerce/curated/daily_product_metrics'
        """
    )

    spark.sql(
        """
        MSCK REPAIR TABLE
        ecommerce.daily_product_metrics
        """
    )

    print(
        "[spark] tabela Hive registrada: "
        "ecommerce.daily_product_metrics"
    )

    print("[spark] consulta Hive:")

    spark.sql(
        f"""
        SELECT
            product_id,
            category,
            purchases,
            revenue,
            conversion_rate_pct,
            reference_date
        FROM ecommerce.daily_product_metrics
        WHERE reference_date = '{args.date}'
        ORDER BY revenue DESC
        """
    ).show(
        50,
        truncate=False
    )

    print(
        "[spark] ETL concluido com sucesso"
    )

    spark.stop()


if __name__ == "__main__":
    main()