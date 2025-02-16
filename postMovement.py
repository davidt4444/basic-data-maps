from pyspark.sql import SparkSession

def main():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("PostToBlogPostMapper") \
        .config("spark.jars", "./mysql-connector-j-8.0.33.jar") \
        .master("local[1]") \
        .getOrCreate()

    try:
        # Read data from Post table in source database
        # posts_df = spark.read.jdbc(url=source_jdbc_url, table="Post", properties=source_properties)
        # Read data from MySQL 'post' table
        with open("../aws-resources/thenameofyourbrand.txt", "r") as file:
            url = file.read().strip()

        # Read from the source table
        posts_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "Post") \
            .load() 

        # Write transformed data to MySQL 'Post_stage' table
        with open("../aws-resources/localhost-mac.txt", "r") as file:
            java_url = file.read().strip()

        posts_df.write \
            .format("jdbc") \
            .option("url", java_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "Post_stage") \
            .mode("append") \
            .save()

        print("Data has been moved from source Post to destination Post_stage successfully.")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Stop spark session
        spark.stop()

if __name__ == "__main__":
    main()