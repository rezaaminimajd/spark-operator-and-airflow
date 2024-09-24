import os

from pyspark.sql import SparkSession

database_user = os.getenv("DATABASE_USER")
database_password = os.getenv("DATABASE_PASSWORD")
database_table = os.getenv("DATABASE_TABLE")
database_host = os.getenv("DATABASE_HOST")
database_port = os.getenv("DATABASE_PORT")

spark = SparkSession.builder.appName("ExampleJob").getOrCreate()

# Your processing logic here...
