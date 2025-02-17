from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, TimestampType, BooleanType
)
from pyspark.sql.functions import col
from typing import Optional
from datetime import datetime

class Post:
    def __init__(self, 
                 id: Optional[int], 
                 uniqueId: str, 
                 title: str, 
                 content: str, 
                 createdAt: datetime, 
                 author: Optional[str], 
                 category: Optional[str], 
                 updatedAt: Optional[datetime], 
                 likesCount: int, 
                 authorId: Optional[int], 
                 isPublished: bool, 
                 views: int):
        self.id = id
        self.uniqueId = uniqueId
        self.title = title
        self.content = content
        self.createdAt = createdAt
        self.author = author
        self.category = category
        self.updatedAt = updatedAt
        self.likesCount = likesCount
        self.authorId = authorId
        self.isPublished = isPublished
        self.views = views

class JPost:
    def __init__(self, 
                 id: Optional[int], 
                 uniqueId: str, 
                 title: str, 
                 content: str, 
                 createdAt: datetime, 
                 author: Optional[str], 
                 category: Optional[str], 
                 updatedAt: Optional[datetime], 
                 likesCount: int, 
                 authorId: Optional[int], 
                 isPublished: bool, 
                 views: int):
        self.id = id
        self.uniqueId = uniqueId
        self.title = title
        self.content = content
        self.createdAt = createdAt
        self.author = author
        self.category = category
        self.updatedAt = updatedAt
        self.likesCount = likesCount
        self.authorId = authorId
        self.isPublished = isPublished
        self.views = views

class PostMapper:
    def __init__(self):
        self.newer = False
        pass

    def post_to_jpost(self):
        spark = SparkSession.builder \
            .appName("PostToBlogPostMapper") \
            .config("spark.jars", "./mysql-connector-j-8.0.33.jar") \
            .master("local[1]") \
            .getOrCreate()

        # Define schema for Post table
        post_schema = StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("uniqueId", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("content", StringType(), nullable=False),
            StructField("createdAt", TimestampType(), nullable=False),
            StructField("author", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("updatedAt", TimestampType(), nullable=True),
            StructField("likesCount", IntegerType(), nullable=False),
            StructField("authorId", IntegerType(), nullable=True),
            StructField("isPublished", BooleanType(), nullable=False),
            StructField("views", IntegerType(), nullable=False)
        ])

        # Read data from MySQL 'post' table
        with open("../aws-resources/localhost-mac-scala.txt", "r") as file:
            url = file.read().strip()

        post_df = spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "post") \
            .load()

        # Map DataFrame to JPost
        def map_row_to_jpost(row):
            id= row.id if self.newer is False else None
            return JPost(
                id=id,
                uniqueId=row.uniqueId,
                title=row.title,
                content=row.content,
                createdAt=row.createdAt,
                author=row.author,
                category=row.category,
                updatedAt=row.updatedAt,
                likesCount=row.likesCount,
                authorId=row.authorId,
                isPublished=row.isPublished,
                views=row.views
            )

        jposts = post_df.rdd.map(map_row_to_jpost)
        jpost_df = spark.createDataFrame(jposts, schema=post_schema)

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

        print("Data has been written to the 'JPost' table.")
        spark.stop()

    def jpost_to_post(self):
        spark = SparkSession.builder \
            .appName("BlogPostToPostMapper") \
            .config("spark.jars", "./mysql-connector-j-8.0.33.jar") \
            .master("local[1]") \
            .getOrCreate()

        # Define schema for JPost table
        jpost_schema = StructType([
            StructField("id", IntegerType(), nullable=True),
            StructField("uniqueId", StringType(), nullable=False),
            StructField("title", StringType(), nullable=False),
            StructField("content", StringType(), nullable=False),
            StructField("createdAt", TimestampType(), nullable=False),
            StructField("author", StringType(), nullable=True),
            StructField("category", StringType(), nullable=True),
            StructField("updatedAt", TimestampType(), nullable=True),
            StructField("likesCount", IntegerType(), nullable=False),
            StructField("authorId", IntegerType(), nullable=True),
            StructField("isPublished", BooleanType(), nullable=False),
            StructField("views", IntegerType(), nullable=False)
        ])

        # Read data from MySQL 'JPost' table
        with open("../aws-resources/localhost-mac-java.txt", "r") as file:
            java_url = file.read().strip()

        jpost_df = spark.read \
            .format("jdbc") \
            .option("url", java_url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "JPost") \
            .load()

        # Map DataFrame to Post
        def map_row_to_post(row):
            id= row.id if self.newer is False else None
            # Handle null values if necessary
            uniqueId = row.uniqueId
            title = row.title if row.title is not None else "Unknown"
            content = row.content if row.content is not None else "No content"
            createdAt = row.createdAt if row.createdAt is not None else datetime.now()
            likesCount = row.likesCount if row.likesCount is not None else 0
            isPublished = row.isPublished if row.isPublished is not None else False
            views = row.views if row.views is not None else 0
            return Post(
                id=id,
                uniqueId=uniqueId,
                title=title,
                content=content,
                createdAt=createdAt,
                author=row.author,
                category=row.category,
                updatedAt=row.updatedAt,
                likesCount=likesCount,
                authorId=row.authorId,
                isPublished=isPublished,
                views=views
            )

        posts = jpost_df.rdd.map(map_row_to_post)
        post_df = spark.createDataFrame(posts, schema=jpost_schema)

        # Write to MySQL 'post' table
        with open("../aws-resources/localhost-mac-scala.txt", "r") as file:
            url = file.read().strip()

        post_df.write \
            .format("jdbc") \
            .option("url", url) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "post") \
            .mode("append") \
            .save()

        print("Data has been written to the 'post' table.")
        spark.stop()
