from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import col, regexp_replace
import psycopg2
import json
import ast
from datetime import datetime
from pyspark.conf import SparkConf

conf = SparkConf()  # create the configuration

spark = SparkSession.builder \
    .appName("Read JSON from Text to PostgreSQL")\
        .config("spark.sql.shuffle.partitions", "16")\
            .config("spark.sql.execution.arrow.enabled", "true")\
                .config("spark.driver.extraClassPath", "file:///D://spark-3.4.0-bin-hadoop3//spark-3.4.0-bin-hadoop3//bin//postgresql-42.6.0.jar") \
                .config("spark.executor.extraClassPath", "file:///D://spark-3.4.0-bin-hadoop3//spark-3.4.0-bin-hadoop3//bin//postgresql-42.6.0.jar") \
                    .getOrCreate()


#Create Database Connection

def create_db_connection(server,database,username,password,port):
    try:
        conn = psycopg2.connect(host=server, dbname=database, user=username, password=password, port=port)
        print('Connection created')
        return conn
    
    except psycopg2.Error as e:
        print(f'Error creating database connection: {e}')

def store_data(spark, df, database, schema, server, port, username, password, table ):
    
    db_properties = {
    "driver": "org.postgresql.Driver",
    "url": "jdbc:postgresql://" + server + ":" + str(port) + "/" + database,
    "user": username,
    "password": password
    }

    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")  # Adjust batch size as per your system's capabilities
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

    print(schema + "." + table, db_properties)
    df.write \
        .option("truncate", "false") \
        .jdbc(db_properties["url"], schema + "." + table, mode="append", properties=db_properties)

#Function to load reviews data to database line by line

def load_reviews_dataset(conn, file_path, database, schema, server, port, username, password, table ):
    try:

        json_schema = StructType() \
            .add("asin", StringType()) \
            .add("helpful", StringType())\
            .add("overall", StringType())\
            .add("reviewText", StringType())\
            .add("reviewTime", StringType())\
            .add("reviewerID", StringType())\
            .add("reviewerName", StringType())\
            .add("summary", StringType())\
            .add("unixReviewTime", StringType())

        lines_df = spark.read.text(file_path)
        json_df = lines_df.select(
            from_json(lines_df.value, json_schema).alias("data")
        ).select("data.*")

        for column in json_df.columns:
            json_df = json_df.withColumn(column, regexp_replace(col(column), "\x00", ""))\
                .withColumn(column, regexp_replace(col(column), "\u0000", ""))


        # Step 3: Define a dictionary with the column names to replace
        column_mapping = {
            "reviewText": "review_text",
            "reviewTime": "review_time",
            "reviewerID": "reviewer_id",
            "reviewerName": "reviewer_name",
            "unixReviewTime": "unix_review_time"
        }

        # Step 4: Replace column names in the DataFrame
        for old_column, new_column in column_mapping.items():
            json_df = json_df.withColumnRenamed(old_column, new_column)

        
        store_data(spark, json_df, database, schema, server, port, username, password, table )


    except (IOError, OSError) as e:
        print(f"Error loading file: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()  # Close the database connection

#Function to load products data to database line by line

def load_products_dataset(conn,file_path,database, schema, server, port, username, password, table):
    try:
 
        json_schema = StructType() \
            .add("asin", StringType()) \
            .add("brand", StringType())\
            .add("categories", StringType())\
            .add("description", StringType())\
            .add("imUrl", StringType())\
            .add("price", StringType())\
            .add("related", StringType())\
            .add("salesRank", StringType())\
            .add("title", StringType())

        lines_df = spark.read.text(file_path)
        json_df = lines_df.select(
            from_json(lines_df.value, json_schema).alias("data")
        ).select("data.*")

        # Step 3: Define a dictionary with the column names to replace
        column_mapping = {
            "imUrl": "im_url",
            "salesRank": "sales_rank"
        }

        # Step 4: Replace column names in the DataFrame
        for old_column, new_column in column_mapping.items():
            json_df = json_df.withColumnRenamed(old_column, new_column)
        
        store_data(spark, json_df, database, schema, server, port, username, password, table )
        
    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e.msg}")
    except Exception as e:
        print(f"Error: {e}")