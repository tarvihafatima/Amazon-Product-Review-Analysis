from pyspark.sql.functions import col, lower, trim, initcap,regexp_replace, \
                                  split,to_date, length, from_json, posexplode, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, ArrayType, \
                              IntegerType, DateType, MapType, TimestampType


def remove_duplicates(df):

    try:

        # Remove duplicates
        df_no_duplicates = df.dropDuplicates()
        return df_no_duplicates
            
    except Exception as e:
        print(f"Error: {e}")
        

def data_type_validation_products(df):
    # Perform data type validation 

    # Define the schema with a StructType
    try:

        products_schema = StructType([
            StructField("asin", StringType(), nullable=False),
            StructField("brand", StringType(), nullable=True),
            StructField("category_level", IntegerType(), nullable=True),
            StructField("category_name", StringType(), nullable=True),
            StructField("description", StringType(), nullable=True),
            StructField("im_url", StringType(), nullable=True),
            StructField("price", DecimalType(10, 2), nullable=True),
            StructField("also_viewed", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("also_bought", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("bought_together", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("buy_after_view", ArrayType(StringType(), containsNull=True), nullable=True),
            StructField("sales_rank", LongType(), nullable=True),
            StructField("sales_rank_category", StringType(), nullable=True),
            StructField("title", StringType(), nullable=True),
            StructField("insertion_time", TimestampType(), nullable=False)
        ])

        # Validate the DataFrame schema against the desired schema
        is_compliant = str(df.schema) == str(products_schema)

        # Print the compliance status
        if is_compliant:
            print("DataFrame schema is compliant with the products schema.")
            return True
        else:
            print("DataFrame schema is not compliant with the products schema.")
            # Optionally, you can print the differences between the two schemas
            print("Desired Schema:")
            print(products_schema)
            print("Actual Schema:")
            print(df.schema)
            return False
        
    except Exception as e:
        print(f"Error: {e}")
        

def data_type_validation_reviews(df):

    try: 

        # Perform data type validation 

        # Define the schema with a StructType
        
        reviews_schema = StructType([
        StructField("asin", StringType(), nullable=True),
        StructField("review_up_votes", IntegerType(), nullable=True),
        StructField("review_interactions", IntegerType(), nullable=True),
        StructField("review_rating", DecimalType(10, 1), nullable=True),
        StructField("review_text", StringType(), nullable=True),
        StructField("review_date", DateType(), nullable=True),
        StructField("reviewer_id", StringType(), nullable=True),
        StructField("reviewer_name", StringType(), nullable=True),
        StructField("review_summary", StringType(), nullable=True),
        StructField("unix_review_time", LongType(), nullable=True),
        StructField("insertion_time", TimestampType(), nullable=False)
        ])

        # Validate the DataFrame schema against the desired schema
        is_compliant = str(df.schema) == str(reviews_schema)

        # Print the compliance status
        if is_compliant:
            print("DataFrame schema is compliant with the reviews schema.")
            return True
        else:
            print("DataFrame schema is not compliant with the reviews schema.")
            # Optionally, you can print the differences between the two schemas
            print("Desired Schema:")
            print(reviews_schema)
            print("Actual Schema:")
            print(df.schema)
            return False
        
    except Exception as e:
        print(f"Error: {e}")
        


def remove_outliers_from_products(df):

    try:
        # Filter the DataFrame to remove rows with price less than 0 
        filtered_df = df.filter(col("price") >= 0)
        
        # Filter the DataFrame based on the length of the asin column
        filtered_df = filtered_df.filter(length("asin") == 10)

        return filtered_df
    
    except Exception as e:
        print(f"Error: {e}")
        


def remove_outliers_from_reviews(df):

    try:

        # Filter the DataFrame to remove rows with rating less than 0 or greater than 5
        filtered_df = df.filter((col("overall") >= 0) & (col("overall") <= 5))

        # #Filter the DataFrame to remove rows where review up votes are greater than review total interactions
        # filtered_df = filtered_df.filter(expr("cast(split(helpful, ', ')[0] as int) <= cast(split(helpful, ', ')[1] as int)"))

        # Filter the DataFrame based on the length of the asin column
        filtered_df = filtered_df.filter(length("asin") == 10)


        # Filter the DataFrame based on the length of the unix_review_time column
        filtered_df = filtered_df.filter((length("unix_review_time") < 11) & (length("unix_review_time") > 8))
        
        return filtered_df
    
    except Exception as e:
        print(f"Error: {e}")
        

def check_misisng_data_products(df):

    try:
        # Check for missing values
        missing_values = df.filter(col("asin").isNull() )
        if missing_values.count() > 0:
            print ("Products Missing Values: ",  missing_values.count())

            # Drop rows with missing values in specific columns
            df.dropna(subset=['asin'])
        return df
    
    except Exception as e:
        print(f"Error: {e}")
        

def check_misisng_data_reviews(df):

    try:
        # Check for missing values
        missing_values = df.filter(col("asin").isNull() | col("overall").isNull()| col("reviewer_id").isNull()| col("unix_review_time").isNull())
        if missing_values.count() > 0:
            raise Exception("Missing values found in asin or overall or reviewer_id or unix_review_time.")
    
        # Drop rows with missing values in specific columns
        df.dropna(subset=['asin', 'unix_review_time'])
        return df
            
    except Exception as e:
        print(f"Error: {e}")
        
    
def set_df_columns_nullable(spark, df, column_list, nullable=True):

    try:
        for struct_field in df.schema:
            if struct_field.name in column_list:
                struct_field.nullable = nullable

        df_mod = spark.createDataFrame(df.rdd, df.schema)
        return df_mod
            
    except Exception as e:
        print(f"Error: {e}")

def clean_and_transform_products(spark, df):

    try:
        # Define the schema for the dictionary
        dictionary_schema = MapType(StringType(), ArrayType(StringType()))

        # Apply the from_json function to cast the string column to a dictionary column
        df = df.withColumn("related", from_json(df["related"], dictionary_schema))
        
        # Extract values from related into separate columns (also_viewed, also_bought, bought_together, buy_after_view)
        df = df.withColumn("also_viewed", col("related").getItem("also_viewed"))\
        .withColumn("also_bought", col("related").getItem("also_bought"))\
        .withColumn("bought_together", col("related").getItem("bought_together"))\
        .withColumn("buy_after_view", col("related").getItem("buy_after_view"))
        
        # Drop the related column
        df = df.drop("related")
            
        # Extract key and value from sales_rank column into separate columns (sales_rank and sales_rank_category)

        df = df.withColumn("sales_rank", regexp_replace(regexp_replace(df.sales_rank, r"[\{\}]", ""), r"\"", ""))
        df = df.withColumn("sales_rank", split(col("sales_rank"), ":"))\
            .withColumn("sales_rank_category", col("sales_rank").getItem(0))\
            .withColumn("sales_rank", col("sales_rank").getItem(1))
        
        # Define the schema for the nested array
        schema = 'array<array<string>>'

        # Convert the string column to a nested array column
        df = df.select("*", from_json('categories', schema).alias('categories1'))

        # Explode the array and get the index and value columns
        df_exploded = df.select("*", posexplode(df.categories1).alias("category_level1", "category_name1"))

        # Explode the inner list to get individual values
        df = df_exploded.select("*",  posexplode("category_name1").alias("category_level", "category_name"))

        # Drop the categories column
        df = df.drop("categories")
        df = df.drop("categories1")
        df = df.drop("category_level1")
        df = df.drop("category_name1")

        #Update Datatypes
        df = df.withColumn("price", col("price").cast(DecimalType(10, 2)))
        df = df.withColumn("sales_rank", col("sales_rank").cast(LongType()))

        # Add a new column with the current date and time
        df = df.withColumn("insertion_time", current_timestamp())

        #Update Nullable 
        df = set_df_columns_nullable(spark, df,['asin'], False)
        df = set_df_columns_nullable(spark, df,['category_level', 'category_name'])

        return df
    

    except Exception as e:
        print(f"Error: {e}")


def clean_and_transform_reviews(spark, df):

    try:

        # Apply consistency transformations to the reviewer name column
        df = df.withColumn("reviewer_name", trim(initcap(lower(col("reviewer_name")))))

    # Split helpful column into two separate columns (review_up_votes and review_interactions)
        df = df.withColumn("helpful", regexp_replace(col("helpful"), "[\\[\\]]", ""))
        df = df.withColumn("helpful", split(col("helpful"), ",").cast("array<int>"))
        df = df.withColumn("review_up_votes", col("helpful")[0])
        df = df.withColumn("review_interactions", col("helpful")[1])

        # Drop the helpful column
        df = df.drop("helpful")

        # Convert the review_time to date format
        df = df.withColumn("review_time", to_date(regexp_replace(col("review_time"), "[\\(\\)]", ""), "MM dd, yyyy"))

        # Rename review_time column to review_date
        df = df.withColumnRenamed("review_time", "review_date")

        # Rename overall column to review_rating
        df = df.withColumnRenamed("overall", "review_rating")
        
        # Rename summary column to review_summary
        df = df.withColumnRenamed("summary", "review_summary")
        
        #Update Datatypes
        df = df.withColumn("review_rating", col("review_rating").cast(DecimalType(10, 1)))
        df = df.withColumn("unix_review_time", col("unix_review_time").cast(LongType()))

        # Add a new column with the current date and time
        df = df.withColumn("insertion_time", current_timestamp())
        
        return df

            
    except Exception as e:
        print(f"Error: {e}")



def rearrange_columns_products(df):

    try:

        # Specify the desired column order

        column_order = ["asin", "brand", "category_level", "category_name", \
                        "description", "im_url", "price", "also_viewed", "also_bought",\
                        "bought_together", "buy_after_view", "sales_rank", "sales_rank_category", "title", "insertion_time"]

        # Rearrange the columns in the DataFrame
        df = df.select(*column_order)

        return df
    
                
    except Exception as e:
        print(f"Error: {e}")



def rearrange_columns_reviews(df):

    try:

        # Specify the desired column order

        print(df.columns)

        column_order = ["asin", "review_up_votes", "review_interactions",\
                        "review_rating", "review_text", "review_date", "reviewer_id", "reviewer_name",\
                        "review_summary", "unix_review_time", "insertion_time"]

        # Rearrange the columns in the DataFrame
        df = df.select(*column_order)

        return df
            
    except Exception as e:
        print(f"Error: {e}")

    