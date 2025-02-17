from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BinaryType, BooleanType
from pyspark.sql.functions import col, lit, length

def py_post_to_cpp_post():
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("PostToBlogPostMapper") \
        .config("spark.jars", "./mysql-connector-j-8.0.33.jar") \
        .master("local[1]") \
        .getOrCreate()

    # Define source schema
    source_schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("uniqueId", StringType(), nullable=False),
        StructField("title", StringType(), nullable=True),
        StructField("author", StringType(), nullable=True),
        StructField("date", TimestampType(), nullable=True),
        StructField("content", BinaryType(), nullable=True)
    ])

    # Read data from MySQL 'post' table
    with open("../aws-resources/localhost-mac.txt", "r") as file:
        url = file.read().strip()

    # Read from the source table
    post_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "Post") \
        .load() \
        .select(
            col("id"),
            col("uniqueId"),
            col("title"),
            col("author"),
            col("date").alias("createdAt"),
            col("content").cast(StringType())
        )

    # Transform data to match the destination schema
    # CHECK constraint for title
    # CHECK constraint for content
    jpost_df = post_df \
        .withColumn("category", lit(None).cast(StringType())) \
        .withColumn("updatedAt", lit(None).cast(TimestampType())) \
        .withColumn("likesCount", lit(0).cast(IntegerType())) \
        .withColumn("authorId", lit(None).cast(IntegerType())) \
        .withColumn("isPublished", lit(True).cast(BooleanType())) \
        .withColumn("views", lit(0).cast(IntegerType())) \
        .filter((length(col("title")) >= 5) & (length(col("title")) <= 200)) \
        .filter(length(col("content")) <= 10000)  

    # Write transformed data to MySQL 'JPost' table
    with open("../aws-resources/localhost-mac-java.txt", "r") as file:
        java_url = file.read().strip()

    jpost_df.write \
        .format("jdbc") \
        .option("url", java_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "JPost") \
        .mode("append") \
        .save()

    spark.stop()

if __name__ == "__main__":
    py_post_to_cpp_post()