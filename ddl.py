from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Trip Data Notification")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 3)
    .getOrCreate()
)

spark.sql(
    """
    CREATE TABLE demo.dwh.users (
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
    PARTITIONED BY (bucket(50, user_sk))
    """
)

spark.sql(
    """
    CREATE TABLE demo.dwh.products (
        prod_sk     string,
        id          bigint,
        category    string,
        price       decimal(6,3),
        title       string,
        vendor      string,
        eff_from    date,
        eff_to      date,
        created_at  timestamp)
    USING iceberg
    PARTITIONED BY (bucket(10, prod_sk), days(created_at))
    """
)

spark.sql(
    """
    CREATE TABLE demo.dwh.orders (
        user_sk     string,
        prod_sk     string,
        id          bigint,
        category    string,
        discount    decimal(4,2),
        quantity    integer,
        created_at  timestamp)
    USING iceberg
    PARTITIONED BY (days(created_at))
    """
)
