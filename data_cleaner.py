import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import BooleanType, DecimalType, LongType, StructType, IntegerType, FloatType, StringType, TimestampNTZType, StructField
from datetime import datetime
import json


input_path = "data.csv"
output = "filtered"

columns = [
           "order_id", "customer_id", "customer_name",
           "product_id","product_name", "product_category",
           "payment_type", "qty", "price", "datetime",
           "country","city","ecommerce_website_name",
           "payment_txn_id","payment_txn_success","failure_reason"
           ]


spark = SparkSession.builder.appName("spas").config("spark.master", "local[*]").getOrCreate()
df = spark.read.csv(input_path, header=True)
df = df.select(*columns)


# now cast each column to a specific type.
df = df.withColumn('order_id', f.col('order_id').cast(LongType()))
df = df.withColumn('customer_id', f.col('customer_id').cast(IntegerType()))
df = df.withColumn('customer_name', f.col('customer_name').cast(StringType()))
df = df.withColumn('product_id', f.col('product_id').cast(LongType()))
df = df.withColumn('product_name', f.col('product_name').cast(StringType()))
df = df.withColumn('product_category', f.col('product_category').cast(StringType()))
df = df.withColumn('payment_type', f.col('payment_type').cast(StringType()))
df = df.withColumn('qty', f.col('qty').cast(IntegerType()))
df = df.withColumn('price', f.col('price').cast(FloatType()))
df = df.withColumn('datetime', f.col('datetime').cast(TimestampNTZType()))
df = df.withColumn('country', f.col('country').cast(StringType()))
df = df.withColumn('city', f.col('city').cast(StringType()))
df = df.withColumn('ecommerce_website_name', f.col('ecommerce_website_name').cast(StringType()))
df = df.withColumn('payment_txn_id', f.col('payment_txn_id').cast(IntegerType()))
df = df.withColumn('payment_txn_success', f.col('payment_txn_success').cast(StringType()))
df = df.withColumn('failure_reason', f.col('failure_reason').cast(StringType()))

#df.select("order_id").show()
#df.select('country').distinct().show()
#df.select('product_category').distinct().show()
#df.select('failure_reason').distinct().show()
#df.select('payment_txn_success').distinct().show()


# drop rows without these columns
df = df.na.drop(subset=['order_id', 'customer_id', 'customer_name'\
                                         ,'product_id', 'product_name', 'product_category'\
                                         ,'payment_type', 'qty', 'price', 'datetime', 'country'\
                                         ,'city', 'ecommerce_website_name', 'payment_txn_id'\
                                         ,'payment_txn_success'\
                                         ])

# drop all duplicate orders and payment ids
df = df.dropDuplicates(['order_id'])
df = df.dropDuplicates(['payment_txn_id'])

unique = {}
for column in df.columns:
    unique_count = df.groupBy(column).count().collect()
    result = {str(r[column]): r['count'] for r in unique_count}
    result = dict(sorted(result.items()))
    for k,v in result.items():
        if v > 1:
            if column == 'order_id':
                print(k," ", v, column)
            if column == 'payment_txn_id':
                print(k, " ", v, column)
    unique[column] = result

with open('count2.json', 'w') as f:
    json.dump(unique, f, indent=4)


df.coalesce(1).write.mode("overwrite").csv(output, header=True)

spark.stop()
