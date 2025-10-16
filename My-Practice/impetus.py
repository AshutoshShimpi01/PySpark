From pyspark.sql.functions import *

Creating Schema Explicitly

Schema = 
StructType([
StructField('product_id', IntegerType(), True),
StructField('Product_name', StringType(), True),
StructField('Price', IntegerType(), True)
])

Reading & load the csv file

df = spark.read.format('csv').option('inferSchema', False).schema(mySchema).load('path').na.fill(0)

Writing the csv

df_update.write.format('parquet').mode('overwrite').save(path')


Increasing price by 10 % category = 'ABC'

df.withColumn('new', when(col('product_name') == 'ABC' , col('price') * 1.10).otherwise(col('price'))).show()
