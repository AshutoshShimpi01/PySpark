

Here are 20 basic PySpark syntax examples useful for everyday tasks:

1. Import functions:
```python
from pyspark.sql.functions import *
```

2. Create SparkSession:
```python
spark = SparkSession.builder.appName("App").getOrCreate()
```

3. Read CSV:
```python
df = spark.read.option("header", True).csv("file.csv")
```

4. Show DataFrame:
```python
df.show()
```

5. Select columns:
```python
df.select("name","salary").show()
```

6. Filter rows:
```python
df.filter(df.age > 30).show()
```

7. Add new column:
```python
df.withColumn("new_col", df.salary*1.1).show()
```

8. Rename column:
```python
df.withColumnRenamed("salary", "income").show()
```

9. Drop column:
```python
df.drop("column_name").show()
```

10. GroupBy and aggregate:
```python
df.groupBy("department").avg("salary").show()
```

11. Sort data:
```python
df.orderBy(df.salary.desc()).show()
```

12. Join DataFrames:
```python
df1.join(df2, df1.id == df2.id).show()
```

13. Cache DataFrame:
```python
df.cache()
```

14. Write DataFrame to CSV:
```python
df.write.mode("overwrite").csv("output_path")
```

15. Register Temp Table for SQL:
```python
df.createOrReplaceTempView("emp")
spark.sql("SELECT * FROM emp WHERE salary > 50000").show()
```

16. Apply UDF (User Defined Function):
```python
from pyspark.sql.functions import udf
def to_upper(s): return s.upper()
upper_udf = udf(to_upper)
df.withColumn("name_upper", upper_udf(df.name)).show()
```

17. Drop duplicates:
```python
df.dropDuplicates().show()
```

18. Limit rows:
```python
df.limit(10).show()
```

19. Convert to Pandas DataFrame:
```python
pandas_df = df.toPandas()
```

20. Read JSON:
```python
json_df = spark.read.json("file.json")
```

These cover many PySpark basics and are great starting points! Let me know if you want examples or explanations for any of these.

[1](https://sparkbyexamples.com/pyspark-tutorial/)
[2](https://docs.databricks.com/aws/en/pyspark/basics)
[3](https://learn.microsoft.com/en-us/azure/databricks/pyspark/basics)
[4](https://spark.apache.org/examples.html)
[5](https://www.sparkplayground.com/pyspark-cheat-sheet)
[6](https://www.projectpro.io/apache-spark-tutorial/pyspark-tutorial)
[7](https://palantir.com/docs/foundry/transforms-python-spark/pyspark-syntax-cheat-sheet/)
[8](https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_df.html)
