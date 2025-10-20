

from pyspark.sql.window import Window
from pyspark.sql.functions import *

#4.  **Running Total:** Calculate the running total of salaries across the entire company, ordering the accumulation based on individual employee salary (from lowest to highest).


r = Window.orderBy(col('salary'))

employees_df.withColumn('new', sum(col('salary').over(r))).show()
