from pyspark.sql import SparkSession
import requests
from pyspark.sql.functions import udf, when
from pyspark.sql.types import DoubleType, StringType
import geohash2 as geohash

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Restaurants Weather Data ETL") \
    .getOrCreate()

# Define the path to the local storage
data_path_1 = "/home/akbota/Desktop/Epam-Spark/restaurant_csv/restaurant_csv/part-00000-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
data_path_2 = "/home/akbota/Desktop/Epam-Spark/restaurant_csv/restaurant_csv/part-00001-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
data_path_3 = "/home/akbota/Desktop/Epam-Spark/restaurant_csv/restaurant_csv/part-00002-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
data_path_4 = "/home/akbota/Desktop/Epam-Spark/restaurant_csv/restaurant_csv/part-00003-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"
data_path_5 = "/home/akbota/Desktop/Epam-Spark/restaurant_csv/restaurant_csv/part-00004-c8acc470-919e-4ea9-b274-11488238c85e-c000.csv"

# Read CSV data into a DataFrames
df_1 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_path_1)

df_2 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_path_2)

df_3 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_path_3)

df_4 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_path_4)

df_5 = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_path_5)

# Combine multiple dataframes into a single dataframe
restaurants_df = df_1.union(df_2).union(df_3).union(df_4).union(df_5)
restaurants_df.show(3)
restaurants_df.printSchema()

# Identify unique fields to be sure that there are no duplicates
distinct_count = restaurants_df.distinct().count()
print(f"Number of distinct rows: {distinct_count}")

# Check restaurant data for incorrect (null) values (latitude and longitude). Filter rows where both latitude and longitude are null
null_lat_lon_df = restaurants_df.filter(restaurants_df.lat.isNull() & restaurants_df.lng.isNull())
# Show the filtered rows
null_lat_lon_df.show()

# Define a Function to Fetch Coordinates
def fetch_coordinates(city, country, api_key):
    location = f"{city}, {country}"
    url = f"https://api.opencagedata.com/geocode/v1/json?q={location}&key={api_key}"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        if data['results']:
            coordinates = data['results'][0]['geometry']
            return coordinates['lat'], coordinates['lng']
    return None, None

api_key = "634a77bada9c4c428af369599d516b97"

# Define the UDF
def get_lat(city, country):
    lat, _ = fetch_coordinates(city, country, api_key)
    return lat

def get_lng(city, country):
    _, lng = fetch_coordinates(city, country, api_key)
    return lng

# Register UDFs
get_lat_udf = udf(get_lat, DoubleType())
get_lng_udf = udf(get_lng, DoubleType())

# Update Null lat and lng Columns. Apply the UDFs to fill in missing lat and lng values.
updated_restaurants_df = restaurants_df.withColumn(
    "lat",
    when(restaurants_df.lat.isNotNull(), restaurants_df.lat)  # Keep existing latitude if not null
    .otherwise(get_lat_udf(restaurants_df["city"], restaurants_df["country"]))
).withColumn(
    "lng",
    when(restaurants_df.lng.isNotNull(), restaurants_df.lng)  # Keep existing longitude if not null
    .otherwise(get_lng_udf(restaurants_df["city"], restaurants_df["country"]))
)
# Filter rows where city is "Dillon"
dillon_row = updated_restaurants_df.filter(updated_restaurants_df.city == "Dillon")

# Show the result
dillon_row.show()

# Define a Function to Generate Geohash. Use the geohash.encode function to create a geohash for given latitude and longitude.
def generate_geohash(lat, lng, precision=4):
    if lat is None or lng is None:
        return None  # Handle null values gracefully
    return geohash.encode(lat, lng, precision=precision)

# Define the UDF
geohash_udf = udf(lambda lat, lng: generate_geohash(lat, lng, precision=4), StringType())

#Add Geohash Column to Restaurants DataFrame
updated_df_with_geohash = updated_restaurants_df.withColumn("geohash", geohash_udf(updated_restaurants_df["lat"], updated_restaurants_df["lng"]))
updated_df_with_geohash.show()

# Define the path to the local storage where weather data stored
data_weather_1 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=29/part-00017-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_2 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=29/part-00018-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_3 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=29/part-00229-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_4 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=30/part-00076-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_5 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=30/part-00077-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_6 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=30/part-00227-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_7 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=31/part-00136-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_8 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=31/part-00137-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"
data_weather_9 = "/home/akbota/Desktop/Epam-Spark/8_October_29-31/weather/year=2016/month=10/day=31/part-00218-44bd3411-fbe4-4e16-b667-7ec0fc3ad489.c000.snappy.parquet"

# Read parquet data into a DataFrames
df_weather_1 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_1)

df_weather_2 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_2)

df_weather_3 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_3)

df_weather_4 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_4)

df_weather_5 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_5)

df_weather_6 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_6)

df_weather_7 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_7)

df_weather_8 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_8)

df_weather_9 = spark.read.format("parquet") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(data_weather_9)

# Combine multiple weather dataframes into a single dataframe
weather_df = df_weather_1.union(df_weather_2).union(df_weather_3).union(df_weather_4).union(df_weather_5) \
                    .union(df_weather_6).union(df_weather_7).union(df_weather_8).union(df_weather_9)

#Add Geohash Column to Weather DataFrame
updated_weather_df_with_geohash = weather_df.withColumn("geohash", geohash_udf(weather_df["lat"], weather_df["lng"]))
updated_weather_df_with_geohash.show()

# Ensure weather_df has unique geohash values. If duplicates exist, deduplicate weather_df.
weather_df = updated_weather_df_with_geohash.dropDuplicates(["geohash"])

# Left-join weather and restaurant data using the four-character geohash.
enriched_df = updated_df_with_geohash.join(weather_df, on="geohash", how="left")

# Define the output path
output_path = "/home/akbota/Desktop/Epam-Spark/enriched_data"

# Write the data in Parquet format with partitioning
enriched_df.write \
    .partitionBy("country", "city") \
    .mode("overwrite") \
    .parquet(output_path)








