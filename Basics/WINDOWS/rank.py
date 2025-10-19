from pyspark.sql.window import Window
from pyspark.sql.functions import *

window_spec = Window.partitionBy('dept_id').orderBy(col('salary').desc())

employees_df.withColumn('rk',rank().over(window_spec)).show()
