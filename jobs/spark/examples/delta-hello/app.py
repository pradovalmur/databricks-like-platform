from pyspark.sql import SparkSession


def main():
    spark = (
        SparkSession.builder
        .appName("delta-hello")
        .getOrCreate()
    )

    data = [
        (1, "Valmur"),
        (2, "Databricks-like"),
        (3, "Delta Lake"),
    ]

    df = spark.createDataFrame(data, ["id", "name"])

    output_path = "s3a://lakehouse/delta/hello"

    df.write.format("delta").mode("overwrite").save(output_path)

    result = spark.read.format("delta").load(output_path)
    result.show()

    spark.stop()


if __name__ == "__main__":
    main()