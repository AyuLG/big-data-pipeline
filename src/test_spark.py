from pyspark.sql import SparkSession

print("Creating Spark session...")
spark = SparkSession.builder \
    .appName("Test") \
    .master("local[*]") \
    .getOrCreate()

print("Spark version:", spark.version, "works!")

# Create a simple DataFrame
df = spark.range(10)
print("Count:", df.count())

spark.stop()
print("✅ Spark test passed!")
