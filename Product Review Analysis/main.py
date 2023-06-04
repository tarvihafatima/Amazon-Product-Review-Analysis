import config.Read_Configs as Read_Configs
import common.Download_Zipped_Files as Download_Zipped_Files
import common.Decompress_Files as Decompress_Files
import common.Load_Raw_Data_into_DB as Load_Raw_Data_into_DB


def main():    
    try:
            source_configs = Read_Configs.read_source_configurations()
            local_configs = Read_Configs.read_local_configurations()
            
            Download_Zipped_Files.download_file_in_threads(source_configs['products'],local_configs['directory'],4) # data fetched by each thread = 4GB
            Download_Zipped_Files.download_file_in_threads(source_configs['reviews'],local_configs['directory'],4) # data fetched by each thread = 4GBs
            Decompress_Files.decompress_file(local_configs['products_zipped'],local_configs['directory'],local_configs['products_filename']) #uncompress product Metadata File
            Decompress_Files.decompress_file(local_configs['reviews_zipped'],local_configs['directory'],local_configs['reviews_filename']) #uncompress review data
            
            db_configs = Read_Configs.read_database_configurations()
            db_connection = Load_Raw_Data_into_DB.create_db_connection(db_configs['server'],db_configs['database'],db_configs['username'],db_configs['password'],db_configs['port']) #Create Database Connection
            Load_Raw_Data_into_DB.load_products_dataset(db_connection, local_configs['products_json']) #load product data into PSQL
            Load_Raw_Data_into_DB.load_reviews_dataset(db_connection, local_configs['reviews_json']) #loads review data into PSQL
                
    except Exception as e:        
        print("Error in main", str(e))

if __name__ == "__main__":
    main()