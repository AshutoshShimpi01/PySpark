from pyspark.sql.window import Window
from pyspark.sql.functions import *

window_spec = Window.partitionBy('dept_id').orderBy(col('salary').desc())

employees_df.withColumn('rk',rank().over(window_spec)).show()





Dense_rank

from pyspark.sql.window import Window
from pyspark.sql.functions import *

window_spec_group_total = Window.partitionBy('dept_id').orderBy(col('salary').desc())

employees_df.withColumn('d_rk', dense_rank().over(window_spec_group_total)).show()
