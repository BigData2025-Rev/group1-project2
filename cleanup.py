from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import json

spark = SparkSession.builder \
    .appName("group1-p2") \
    .getOrCreate()

df = spark.read.csv("data.csv", header=True, inferSchema=True)

# df.first()
# df.printSchema()

df = df.na.drop(subset=["product_category", "payment_type"])

df = df.withColumn("country", F.lower("country"))

unique = {}
for column in df.columns:
    unique_count = df.groupBy(column).count().collect()
    result = {str(r[column]): r['count'] for r in unique_count}
    result = dict(sorted(result.items()))
    unique[column] = result

with open('count2.json', 'w') as f:
    json.dump(unique, f, indent=4)
