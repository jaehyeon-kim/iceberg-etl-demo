from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.functions import md5, concat, to_timestamp, to_date, expr, col


def create_tables(
    *, tbls: List[str] = ["users", "products", "orders"], spark_session: SparkSession
):
    if len(set(tbls) - set(["users", "products", "orders"])) > 0:
        raise Exception("supported tables are users, products or orders")
    for tbl in tbls:
        print(f"creating demo.dwh.{tbl}...")
        spark_session.sql(f"DROP TABLE IF EXISTS demo.dwh.{tbl}")
        if tbl == "users":
            spark_session.sql(
                f"""
                CREATE TABLE demo.dwh.{tbl} (
                    user_sk     string,
                    id          bigint,
                    name        string,
                    email       string,
                    address     string,
                    city        string,
                    state       string,
                    zip         string,
                    birth_date  date,
                    source      string,
                    created_at  timestamp)
                USING iceberg
                PARTITIONED BY (bucket(20, user_sk))
                """
            )
        elif tbl == "products":
            spark_session.sql(
                f"""
                CREATE TABLE demo.dwh.{tbl} (
                    prod_sk     string,
                    id          bigint,
                    category    string,
                    price       decimal(6,3),
                    title       string,
                    vendor      string,
                    curr_flag   int,
                    eff_from    timestamp,
                    eff_to      timestamp,
                    created_at  timestamp)
                USING iceberg
                PARTITIONED BY (bucket(20, prod_sk))
                """
            )
        else:
            spark_session.sql(
                f"""
                CREATE TABLE demo.dwh.{tbl} (
                    user_sk     string,
                    prod_sk     string,
                    id          bigint,
                    discount    decimal(4,2),
                    quantity    integer,
                    created_at  timestamp)
                USING iceberg
                PARTITIONED BY (days(created_at))
                """
            )


def upsert_users(file_path: str, spark_session: SparkSession):
    print("users - read records...")
    src_df = (
        spark_session.read.option("header", "true")
        .csv(file_path)
        .withColumn("user_sk", md5("id"))
        .withColumn("id", expr("CAST(id AS bigint)"))
        .withColumn("created_at", to_timestamp("created_at"))
        .withColumn("birth_date", to_date("birth_date"))
        .select(
            "user_sk",
            "id",
            "name",
            "email",
            "address",
            "city",
            "state",
            "zip",
            "birth_date",
            "source",
            "created_at",
        )
    )
    print("users - transform and upsert records...")
    src_df.createOrReplaceTempView("users_tbl")
    spark_session.sql(
        """
        MERGE INTO demo.dwh.users t
        USING (SELECT * FROM users_tbl ORDER BY user_sk) s
        ON s.user_sk = t.user_sk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def upsert_products(file_path: str, spark_session: SparkSession):
    print("products - read records...")
    src_df = (
        spark_session.read.option("header", "true")
        .csv(file_path)
        .withColumn("prod_sk", md5(concat("id", "created_at")))
        .withColumn("id", expr("CAST(id AS bigint)"))
        .withColumn("price", expr("CAST(price AS decimal(6,3))"))
        .withColumn("created_at", to_timestamp("created_at"))
        .withColumn("curr_flag", expr("CAST(NULL AS int)"))
        .withColumn("eff_from", col("created_at"))
        .withColumn("eff_to", expr("CAST(NULL AS timestamp)"))
        .select(
            "prod_sk",
            "id",
            "category",
            "price",
            "title",
            "vendor",
            "curr_flag",
            "eff_from",
            "eff_to",
            "created_at",
        )
    )
    print("products - transform and upsert records...")
    src_df.createOrReplaceTempView("products_tbl")
    products_update_qry = """
    WITH products_to_update AS (
        SELECT l.* 
        FROM demo.dwh.products AS l
        JOIN products_tbl AS r ON l.id = r.id
        UNION
        SELECT *
        FROM products_tbl
    ), products_updated AS (
        SELECT *,
                LEAD(created_at) OVER (PARTITION BY id ORDER BY created_at) AS eff_lead
        FROM products_to_update
    )
    SELECT prod_sk, 
            id, 
            category, 
            price, 
            title, 
            vendor,
            (CASE WHEN eff_lead IS NULL THEN 1 ELSE 0 END) AS curr_flag,
            eff_from,
            COALESCE(eff_lead, to_timestamp('9999-12-31 00:00:00')) AS eff_to,
            created_at
    FROM products_updated
    ORDER BY prod_sk
    """
    spark_session.sql(
        f"""
        MERGE INTO demo.dwh.products t
        USING ({products_update_qry}) s
        ON s.prod_sk = t.prod_sk
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def upsert_orders(file_path: str, spark_session: SparkSession):
    print("orders - read records...")
    src_df = (
        spark_session.read.option("header", "true")
        .csv(file_path)
        .withColumn("id", expr("CAST(id AS bigint)"))
        .withColumn("user_id", expr("CAST(user_id AS bigint)"))
        .withColumn("product_id", expr("CAST(product_id AS bigint)"))
        .withColumn("discount", expr("CAST(discount AS decimal(4,2))"))
        .withColumn("quantity", expr("CAST(quantity AS int)"))
        .withColumn("created_at", to_timestamp("created_at"))
    )
    print("orders - transform and append records...")
    src_df.createOrReplaceTempView("orders_tbl")
    spark_session.sql(
        """
        WITH src_products AS (
            SELECT * FROM demo.dwh.products
        ), orders_updated AS (
            SELECT o.*, u.user_sk, p.prod_sk
            FROM orders_tbl o
            LEFT JOIN demo.dwh.users u 
                ON o.user_id = u.id
            LEFT JOIN src_products p 
                ON o.product_id = p.id
                AND o.created_at >= p.eff_from
                AND o.created_at < p.eff_to
        ), products_tbl AS (
            SELECT prod_sk, 
                   id,
                   ROW_NUMBER() OVER (PARTITION BY id ORDER BY eff_from) AS rn
            FROM src_products
        )
        SELECT o.user_sk,
               COALESCE(o.prod_sk, p.prod_sk) AS prod_sk,
               o.id,
               o.discount,
               o.quantity,
               o.created_at
        FROM orders_updated AS o
        JOIN products_tbl AS p ON o.product_id = p.id
        WHERE p.rn = 1
        ORDER BY o.created_at
        """
    ).writeTo("demo.dwh.orders").append()
