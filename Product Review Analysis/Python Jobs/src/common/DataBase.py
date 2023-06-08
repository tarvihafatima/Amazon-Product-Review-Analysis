from pyspark.sql.utils import AnalysisException, PythonException

class Database:

    def __init__(self, spark, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.spark = spark
        self.postgres_url = None


    def set_postgres_url_properties(self):

        # Set Postgres credentials and connection properties
        self.postgres_url = "jdbc:postgresql://" + self.host + ":" + str(self.port) + "/" + self.database


    def extract_data(self, schema, table_name):
        
        # Extract data from Postgres table
         
        try:
            df = self.spark.read \
                .format("jdbc") \
                .option("url", self.postgres_url + "?currentSchema=" + schema+ "&user=" + self.user + "&password=" + self.password) \
                .option("dbtable", schema + "." + table_name) \
                .load()

            return df

        except (AnalysisException, PythonException) as e:
            print(f"Exception occurred while reading data from DB: {str(e)}")


    def store_data(self, df, schema, table):

        #Function to stote data in database 
        
        try:

            db_properties = {
            "driver": "org.postgresql.Driver",
            "url": "jdbc:postgresql://" + self.host + ":" + str(self.port) + "/" + self.database,
            "user": self.user,
            "password": self.password
            }

            self.spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", "50000")  # Adjust batch size as per your system's capabilities
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")

            df.write \
                .option("truncate", "false") \
                .jdbc(db_properties["url"], schema + "." + table, mode="append", properties=db_properties)

        except (AnalysisException, PythonException) as e:
            print(f"Exception occurred while storing data into DB: {str(e)}")