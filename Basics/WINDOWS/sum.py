
from pyspark.sql.window import Window
from pyspark.sql.functions import *

window_spec_group_total = Window.partitionBy('dept_id')

employees_df.withColumn('salary',sum(col('salary')).over(window_spec_group_total)).show()
