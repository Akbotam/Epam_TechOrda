from pyspark.sql import SparkSession
import geohash2 as geohash
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def test_data():
   spark = SparkSession.builder.master("local").appName("Test").getOrCreate()
   data = [(39.506226, -106.037933), (None, None)]
   columns = ["lat", "lng"]
   df = spark.createDataFrame(data, columns)
   expected = [geohash.encode(39.506226, -106.037933, precision=4), None]
   geohash_udf = udf(lambda lat, lng: geohash.encode(lat, lng, precision=4) if lat and lng else None, StringType())
   df = df.withColumn("geohash", geohash_udf(df["lat"], df["lng"]))
   # Checking
   result = [row["geohash"] for row in df.collect()]
   assert result == expected