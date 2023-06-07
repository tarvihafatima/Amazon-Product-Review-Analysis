from pyspark.sql import SparkSession
import config.Read_Configs as Read_Configs
import common.Download_Zipped_Files as Download_Zipped_Files
import common.Decompress_Files as Decompress_Files
import common.Load_Raw_Data_into_DB as Load_Raw_Data_into_DB
import common.Transform_and_Load_Data_into_Staging_DB as Transform_and_Load_Data_into_Staging_DB

def main():    
    try:


#-------------------------------------------------
#Read and Setup CONFIGURATIONS
#--------------------------------------------------

        # Create Spark session
        spark = SparkSession.builder \
            .appName("Data Extraction and Transformation") \
            .config("spark.cores.max", "30")\
            .config("spark.sql.shuffle.partitions", "16")\
            .config("spark.executor.memory", "32g") \
            .config("spark.driver.memory", "50g") \
            .config('spark.driver.maxResultSize', '50G')\
            .config("spark.executor.memory_overhead", "16g")\
            .getOrCreate()
        
        spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")  # Adjust batch size as per your system's capabilities
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
        #Read Configurations

        source_configs = Read_Configs.read_source_configurations()
        local_configs = Read_Configs.read_local_configurations()
        db_configs, landing, staging, landing_tables, staging_tables = Read_Configs.read_database_configurations()

        

#-------------------------------------------------
#Setup Connection
#--------------------------------------------------


        #Create DB Connection                   
            
        # Get Postgres Connection Properties and URL

        postgres_url= Transform_and_Load_Data_into_Staging_DB.get_postgres_url_properties(db_configs['database'], db_configs['server'], db_configs['port'])

        db_connection = Load_Raw_Data_into_DB.create_db_connection(db_configs['server'],db_configs['database'],db_configs['username'],db_configs['password'],db_configs['port']) #Create Database Connection
        


    
#-------------------------------------------------
#Landing Layer
#--------------------------------------------------


        # Move Products Data to Landing Stage

        Download_Zipped_Files.download_file_in_threads(source_configs['products'],local_configs['directory'],4) # data fetched by each thread = 4GB
        Decompress_Files.decompress_file(local_configs['products_zipped'],local_configs['directory'],local_configs['products_filename']) #uncompress product Metadata File
        Load_Raw_Data_into_DB.load_products_dataset(db_connection, local_configs['products_json'], db_configs['database'], landing['schema'], db_configs['server'], db_configs['port'], db_configs['username'], db_configs['password'], landing_tables['review']) #load product data into PSQL

        # Move Reviews Data to Landing Stage      
            
        Download_Zipped_Files.download_file_in_threads(source_configs['reviews'],local_configs['directory'],4) # data fetched by each thread = 4GBs
        Decompress_Files.decompress_file(local_configs['reviews_zipped'],local_configs['directory'],local_configs['reviews_filename']) #uncompress review data
        Load_Raw_Data_into_DB.load_reviews_dataset(db_connection, local_configs['reviews_json'], db_configs['database'], landing['schema'], db_configs['server'], db_configs['port'], db_configs['username'], db_configs['password'], landing_tables['product']) #loads review data into PSQL
        

        

    
#-------------------------------------------------
#Staging Layer
#--------------------------------------------------



        # Move Products Data to Staging

        # Extract data from Postgres

        products_data = Transform_and_Load_Data_into_Staging_DB.extract_data(spark, postgres_url, landing['schema'], landing_tables['product'], db_configs['username'],db_configs['password'])

        # Check for missing data
        products_data = Transform_and_Load_Data_into_Staging_DB.check_misisng_data_products(products_data)

        # Remove duplicates
        products_data = Transform_and_Load_Data_into_Staging_DB.remove_duplicates(products_data)

        #Filter Data
        products_data = Transform_and_Load_Data_into_Staging_DB.remove_outliers_from_products(products_data)

        # Clean and transform the data
        products_data = Transform_and_Load_Data_into_Staging_DB.clean_and_transform_products(spark, products_data)

        #Rearrange columns
        products_data = Transform_and_Load_Data_into_Staging_DB.rearrange_columns_products(products_data)

        #Remove Duplicates
        products_data = Transform_and_Load_Data_into_Staging_DB.remove_duplicates(products_data)

        #Data Compliance
        Transform_and_Load_Data_into_Staging_DB.data_type_validation_products(products_data)

        #Store Data in Database
        # store_data(spark, reviews_data, db_configs['database'], staging['schema'], db_configs['server'], db_configs['port'], db_configs['username'], db_configs['password'], staging_tables['review'])
        

        # Move Reviews Data to Staging

        #Extract data from Postgres
        reviews_data = Transform_and_Load_Data_into_Staging_DB.extract_data(spark, postgres_url, landing['schema'], landing_tables['review'], db_configs['username'],db_configs['password'])

        # Check for missing data
        Transform_and_Load_Data_into_Staging_DB.check_misisng_data_reviews(reviews_data)

        # Remove duplicates
        reviews_data = Transform_and_Load_Data_into_Staging_DB.remove_duplicates(reviews_data)

        #Filter Data
        reviews_data = Transform_and_Load_Data_into_Staging_DB.remove_outliers_from_reviews(reviews_data)

        # Clean and transform the data
        reviews_data = Transform_and_Load_Data_into_Staging_DB.clean_and_transform_reviews(spark, reviews_data)

        #Rearrange columns
        reviews_data = Transform_and_Load_Data_into_Staging_DB.rearrange_columns_reviews(reviews_data)

        #Remove Duplicates
        reviews_data = Transform_and_Load_Data_into_Staging_DB.remove_duplicates(reviews_data)
        

        #Data Compliance
        Transform_and_Load_Data_into_Staging_DB.data_type_validation_reviews(reviews_data)

        # #Store Data in Database
        Transform_and_Load_Data_into_Staging_DB.store_data(spark, reviews_data, db_configs['database'], staging['schema'], db_configs['server'], db_configs['port'], db_configs['username'], db_configs['password'], staging_tables['review'] )

        row_count = reviews_data.count()
        print("Number of rows: ", row_count)
        #Stop the Spark session

        spark.stop()

    except Exception as e:        
        print("Error in main", str(e))

if __name__ == "__main__":
    main()