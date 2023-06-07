from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import col, regexp_replace
import json


def load_reviews_dataset(spark, file_path ):

    #Function to read reviews data to database line by line

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


        # Define a dictionary with the column names to replace
        column_mapping = {
            "reviewText": "review_text",
            "reviewTime": "review_time",
            "reviewerID": "reviewer_id",
            "reviewerName": "reviewer_name",
            "unixReviewTime": "unix_review_time"
        }

        # Replace column names in the DataFrame
        for old_column, new_column in column_mapping.items():
            json_df = json_df.withColumnRenamed(old_column, new_column)

        return json_df
       
        
    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e.msg}")
    except Exception as e:
        print(f"Error: {e}")


def load_products_dataset(spark, file_path):

    #Function to load products data to database line by line

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

        # Define a dictionary with the column names to replace
        column_mapping = {
            "imUrl": "im_url",
            "salesRank": "sales_rank"
        }

        # Replace column names in the DataFrame
        for old_column, new_column in column_mapping.items():
            json_df = json_df.withColumnRenamed(old_column, new_column)
        
        return json_df
        
    except FileNotFoundError as e:
        print(f"Error: File not found: {e.filename}")
    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON: {e.msg}")
    except Exception as e:
        print(f"Error: {e}")