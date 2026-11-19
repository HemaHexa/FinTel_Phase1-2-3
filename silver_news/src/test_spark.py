import os
import sys
 
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
from pyspark.sql import SparkSession
 
# Create a Spark session
spark = SparkSession.builder \
    .appName("SilverNewsTest") \
    .master("local[*]") \
    .getOrCreate()
 
# Create sample news data
data = [
    (1, "Reliance shares rise today"),
    (2, "TCS announces strong quarterly results"),
    (3, "Infosys expands its AI business")
]
 
columns = ["article_id", "headline"]
 
df = spark.createDataFrame(data, columns)
 
# Display the data
df.show()
 
# Stop Spark
spark.stop()