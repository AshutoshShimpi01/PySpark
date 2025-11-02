

#8.  **First Value in Partition:** For every employee, display the salary of the **highest-paid employee** in their respective department 
on the same row, using the `first()` window function and ordering by salary descending.


from pyspark.sql.functions import *
from pyspark.sql.window import *

win = Window.partitionBy('dept_id').orderBy(col('salary').desc())
employees_df.withColumn('new', first('salary').over(win)).show()
