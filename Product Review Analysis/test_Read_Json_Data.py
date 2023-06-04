from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, col

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Read the JSON file as a DataFrame
df = spark.read.json("D:\Product Review Analysis\dataset\item_dedup.json")

# Recursive function to extract levels of keys and values
def get_levels(data_frame, prefix='', levels=None):
    if levels is None:
        levels = []

    for column in data_frame.columns:
        new_key = f'{prefix}.{column}' if prefix else column
        levels.append(new_key)

        if data_frame.schema[column].dataType in ('struct', 'array'):
            if any(subtype in data_frame.schema[column].dataType for subtype in ('struct', 'array')):
                nested_df = data_frame.select(column)
                nested_df = nested_df.withColumn(column, explode(col(column)))
                get_levels(nested_df, prefix=new_key, levels=levels)
        else:
            if column != "_corrupt_record":
                
                levels.append(data_frame.select(column).first()[0])

    return levels

# Get all levels of keys and values
levels = get_levels(df)

# Print the levels
for level in levels:
    print(level)

