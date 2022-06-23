# add Iceberg dependency
ICEBERG_VERSION=0.13.2
DEPENDENCIES="org.apache.iceberg:iceberg-spark3-runtime:$ICEBERG_VERSION"

# add AWS dependnecy
AWS_SDK_VERSION=2.17.131
AWS_MAVEN_GROUP=software.amazon.awssdk
AWS_PACKAGES=(
    "bundle"
    "url-connection-client"
)
for pkg in "${AWS_PACKAGES[@]}"; do
    DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

# start Spark SQL client shell
pyspark --packages $DEPENDENCIES \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.demo.warehouse=s3://iceberg-etl-demo \
  --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
  --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO