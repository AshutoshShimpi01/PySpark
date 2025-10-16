

### 1. Transformations and Actions
Transformations (like `filter()`, `select()`) create new DataFrames and are lazily evaluated (not executed immediately). Actions (like `show()`, `count()`) trigger the actual computation.

**Example:**
```python
df_filtered = df.filter(df.age > 30)  # Transformation (lazy)
df_filtered.show()  # Action (triggers execution)
```

***

### 2. Spark SQL
Spark SQL lets you run SQL queries on DataFrames or tables. It integrates SQL querying with Spark’s distributed processing.

**Example:**
```python
df.createOrReplaceTempView("people")
spark.sql("SELECT name, age FROM people WHERE age > 30").show()
```

***

### 3. DataFrame Operations
You can select columns, filter rows, aggregate, and join DataFrames for complex data manipulation.

**Example:**
```python
filtered = df.select("name", "age").filter(df.age > 30)
filtered.groupBy("age").count().show()
```

***

### 4. Partitioning & Bucketing
Partitioning splits data across folders by column values; bucketing groups similar keys into fixed buckets. Both improve query speed.

**Example:**
```python
df.write.partitionBy("year").bucketBy(4, "id").saveAsTable("bucketed_table")
```

***

### 5. Performance Tuning
Use caching, broadcasting, and proper partitioning to make Spark jobs faster and resource-efficient.

**Example:**
```python
df.cache()  # Cache DataFrame in memory
broadcast_var = spark.sparkContext.broadcast(small_lookup_dict)
```

***

### 6. User-Defined Functions (UDFs)
Create custom functions in Python and register them to use in DataFrame operations.

**Example:**
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def upper_case(s):
    return s.upper()

upper_udf = udf(upper_case, StringType())
df.withColumn("upper_name", upper_udf(df.name)).show()
```

***

### 7. Streaming Data with Spark Streaming
Process live data streams (like Kafka) continuously for real-time insights.

**Example:**
```python
stream_df = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()
stream_df.writeStream.format("console").start().awaitTermination()
```

***

### 8. Machine Learning with MLlib
Use MLlib for distributed machine learning algorithms like classification, regression, clustering.

**Example:**
```python
from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(maxIter=10, regParam=0.01)
model = lr.fit(training_data)
predictions = model.transform(test_data)
predictions.show()
```

***

### 9. Debugging & Error Handling
Use Spark UI, logs, and exception handling to debug and resolve issues.

**Example:**
```python
try:
    df.select("non_existent_column").show()
except Exception as e:
    print(f"Error: {e}")
```

***

If you want, I can provide detailed examples or exercises on any topic. Just ask!

[1](https://www.kaggle.com/code/slythe/pyspark-basic-actions-and-transformations-rdd)
[2](https://sparkbyexamples.com/pyspark/pyspark-rdd-transformations/)
[3](https://www.linkedin.com/pulse/transformation-actions-apache-spark-sanyam-jain-a9q7e)
[4](https://supergloo.com/pyspark/apache-spark-transformations-python-examples/)
[5](https://blog.devgenius.io/demystifying-pyspark-a-comprehensive-guide-to-dataframe-transformations-and-actions-483403de9079)
[6](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
[7](https://community.databricks.com/t5/women-in-data-ai/%F0%9D%90%AD%F0%9D%90%AB%F0%9D%90%9A%F0%9D%90%A7%F0%9D%90%AC%F0%9D%90%9F%F0%9D%90%A8%F0%9D%90%AB%F0%9D%90%A6%F0%9D%90%9A%F0%9D%90%AD%F0%9D%90%A2%F0%9D%90%A8%F0%9D%90%A7%F0%9D%90%AC-%F0%9D%90%9A%F0%9D%90%A7%F0%9D%90%9D-%F0%9D%90%9A%F0%9D%90%9C%F0%9D%90%AD%F0%9D%90%A2%F0%9D%90%A8%F0%9D%90%A7%F0%9D%90%AC-%F0%9D%90%AE%F0%9D%90%AC%F0%9D%90%9E%F0%9D%90%9D-%F0%9D%90%A2%F0%9D%90%A7-%F0%9D%90%80%F0%9D%90%A9%F0%9D%90%9A%F0%9D%90%9C%F0%9D%90%A1%F0%9D%90%9E-%F0%9D%90%92%F0%9D%90%A9%F0%9D%90%9A%F0%9D%90%AB%F0%9D%90%A4/td-p/81450)
