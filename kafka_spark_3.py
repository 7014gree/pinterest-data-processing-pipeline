import os
from pyspark.sql import SparkSession
import findspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import from_json, col, regexp_replace, length, max, when
from datetime import datetime

findspark.init()

# Download spark sql kakfa package from Maven repository and submit to PySpark at runtime. 
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1,org.postgresql:postgresql:42.6.0 pyspark-shell' # find what postgres version

# Kafka topic/server details
kafka_topic_name = "PinterestData"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("KafkaStreaming ") \
        .getOrCreate()

# Only display Error messages in the console.
spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
stream_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "earliest") \
        .load()

stream_df = stream_df.selectExpr("CAST(value as STRING)")

print(f"Loading Kafka events from topic {kafka_topic_name} at {kafka_bootstrap_servers} to PySpark DataFrame...")

stream_schema = StructType([StructField('category', StringType(), True),
                      StructField('index', IntegerType(), True),
                      StructField('unique_id', StringType(), True),
                      StructField('title', StringType(), True),
                      StructField('description', StringType(), True),
                      StructField('poster_name', StringType(), True),
                      StructField('follower_count', StringType(), True),
                      StructField('tag_list', StringType(), True),
                      StructField('is_image_or_video', StringType(), True),
                      StructField('image_src', StringType(), True),
                      StructField('downloaded', IntegerType(), True),
                      StructField('save_location', StringType(), True)])

# Load JSON into individual columns per the schema above
df = stream_df.withColumn('temp', from_json(col='value', schema=stream_schema)) \
             .selectExpr('temp.*')

# Change 'k' at end of string to thousands, 'M' at end of string to millions, set to -1 if there is a User Info Error, cast column to INT
cleaned_df = df.withColumn('follower_count', regexp_replace(col('follower_count'), 'k', '000')) \
                .withColumn('follower_count', regexp_replace(col('follower_count'), 'M', '000000')) \
                .withColumn('follower_count', regexp_replace(col('follower_count'),'User Info Error','-1')) \
                .withColumn('follower_count', col('follower_count').cast('int'))

# Remove hyphens in category names
cleaned_df = cleaned_df.withColumn('category', regexp_replace(col('category'), '-', ' '))

# Rename missing titles to something more concise
cleaned_df = cleaned_df.withColumn('title', regexp_replace(col('title'), 'No Title Data Available', '')) \
                        .withColumn('title', when(col('title') == '', None).otherwise(col('title')))

# Set to NULL when there is no description
cleaned_df = cleaned_df.withColumn('description', regexp_replace(col('description'), 'No description available Story format', '')) \
                        .withColumn('description', when(col('description') == '', None).otherwise(col('description')))

# Set to NULL when there is no description. Made string shorter by removing the website name
cleaned_df = cleaned_df.withColumn('image_src', regexp_replace(col('image_src'), 'Image src error.', '')) \
                        .withColumn('image_src', regexp_replace(col('image_src'), 'https://i.pinimg.com/', '')) \
                        .withColumn('image_src', when(col('image_src') == '', None).otherwise(col('image_src')))

# Remove unnecessary 'Local save in ' at the start of every string
cleaned_df = cleaned_df.withColumn('save_location', regexp_replace(col('save_location'), 'Local save in ', '')) \
                        .withColumn('save_location', when(col('save_location') == '', None).otherwise(col('save_location')))

# Make the string for stories shorter
cleaned_df = cleaned_df.withColumn('is_image_or_video', regexp_replace(col('is_image_or_video'), 'multi-video\\(story page format\\)', 'story'))

# Make tag_list NULL when there are no tags
cleaned_df = cleaned_df.withColumn('tag_list', regexp_replace(col('tag_list'), 'N\\,o\\, \\,T\\,a\\,g\\,s\\, \\,A\\,v\\,a\\,i\\,l\\,a\\,b\\,l\\,e', '')) \
                        .withColumn('tag_list', when(col('tag_list') == '', None).otherwise(col('tag_list')))

# Cast as boolean for use in Postgres
cleaned_df = cleaned_df.withColumn('downloaded', col('downloaded').cast('boolean'))

## Used for initial implementation where uuid was the primary key.
## Adding this step increased processing time by a lot and it's not clear how much value was being added.
## Now just streams all data into the database where duplicate UUIDs can be removed later
## cleaned_df = cleaned_df.drop_duplicates()


# Used for finding the largest string lengths, for use in setting up database scheme (length of VARCHARS)
def find_max_str_length(df, id, column_names):
    for column_name in column_names:
        output_df = df.groupBy("category") \
                            .agg(max(length(column_name)).alias(f"{column_name}_length")) \
                            .orderBy(f"{column_name}_length", ascending=False)
        output_df.show()

def write_to_console(df, id):
    df.write.format("console") \
            .mode("append") \
            .option("truncate","true") \
            .save()

# overwrite mode used for clashes in unique id
def write_to_postgres(df, id):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Writing batch to postgres...")
    df.write.format("jdbc").options(
        url="jdbc:postgresql://localhost:5432/pinterest_streaming",
        driver="org.postgresql.Driver",
        dbtable="experimental_data",
        user="postgres",
        password="7014gree",
        stringtype="unspecified").mode("append").save()
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Successfully written batch to postgres.")

# For use with find_max_str_length
# Change .foreachBatch() to .foreachBatch(lambda df, id: find_max_str_length(df, id, columns_to_summarise)) \
# From initial data loaded: description = 179, category = 14, title = 151, tag_list = 234, image_src= 81, poster_name = null, save_location = 20
columns_to_summarise = ['description', 'category', 'title', 'tag_list', 'image_src', 'poster_name', 'save_location']

cleaned_df \
    .writeStream \
    .foreachBatch(write_to_postgres) \
    .start() \
    .awaitTermination()
