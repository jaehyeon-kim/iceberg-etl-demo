from pyspark.sql import SparkSession
from src import create_tables, etl_users, etl_products, etl_orders

spark = SparkSession.builder.appName("Iceberg ETL Demo").getOrCreate()

## create all tables - demo.dwh.users, demo.dwh.products and demo.dwh.orders
create_tables(spark_session=spark)

## users etl - assuming SCD type 1
etl_users("./data/users.csv", spark)

## incremental ETL
for yr in range(0, 4):
    print(f"processing year {yr}")
    ## products etl - assuming SCD type 2
    etl_products(f"./data/products_year_{yr}.csv", spark)
    ## orders etl - relevant user_sk and prod_sk are added during transformation
    etl_orders(f"./data/orders_year_{yr}.csv", spark)
