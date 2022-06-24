from pyspark.sql import SparkSession
from src import create_tables, upsert_users, upsert_products, upsert_orders

spark = SparkSession.builder.appName("Iceberg ETL Demo").getOrCreate()

## create all tables - demo.dwh.users, demo.dwh.products and demo.dwh.orders
create_tables(spark_session=spark)

## upsert users - assuming SCD type 1
upsert_users("./data/users.csv", spark)

## incremental ETL
## upsert products - assuming SCD type 2
## upsert orders - relevant user_sk and prod_sk are taken as primary keys
for yr in range(0, 4):
    print(f"processing year {yr}")
    upsert_products(f"./data/products_year_{yr}.csv", spark)
    upsert_orders(f"./data/orders_year_{yr}.csv", spark)
