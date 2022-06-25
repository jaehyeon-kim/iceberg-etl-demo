#!/usr/bin/env bash

# add Iceberg dependency
ICEBERG_VERSION=0.13.2
DEPENDENCIES="org.apache.iceberg:iceberg-spark3-runtime:$ICEBERG_VERSION"

# add AWS dependency
AWS_SDK_VERSION=2.17.131
AWS_MAVEN_GROUP=software.amazon.awssdk
AWS_PACKAGES=(
    "bundle"
    "url-connection-client"
)
for pkg in "${AWS_PACKAGES[@]}"; do
    DEPENDENCIES+=",$AWS_MAVEN_GROUP:$pkg:$AWS_SDK_VERSION"
done

# execute pyspark or spark-submit
execution=$1
app_path=$2
if [ -z $execution ]; then
    echo "missing execution type. specify either pyspark or spark-submit"
    exit 1
fi

if [ $execution == "pyspark" ]; then
    pyspark --packages $DEPENDENCIES \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.demo.warehouse=s3://iceberg-etl-demo \
        --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
        --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO
elif [ $execution == "spark-submit" ]; then
    if [ -z $app_path ]; then
        echo "pyspark application is mandatory"
        exit 1
    else
        spark-submit --packages $DEPENDENCIES \
            --deploy-mode client \
            --master local[*] \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.demo=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.demo.warehouse=s3://iceberg-etl-demo \
            --conf spark.sql.catalog.demo.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
            --conf spark.sql.catalog.demo.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
            $app_path
    fi
fi
