


from pyspark.sql.window import Window
from pyspark.sql.functions import *

#1.  **Top N Per Group (Dense Rank):** Find the top 2 highest-paid employees in each department and display their name, department name, and salary. Use the `dense_rank()` window function.

r = Window.partitionBy('dept_id').orderBy(col('salary').desc())

employees_df.withColumn('rk', dense_rank().over(r)).filter(col('rk') < 3).show()
