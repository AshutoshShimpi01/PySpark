

from pyspark.sql.window import Window
from pyspark.sql.functions import *

#**Relative Salary Comparison (Avg):** Calculate a new column showing the difference between each employee's salary and the average salary of their respective department.

r = Window.partitionBy('dept_id')

employees_df.withColumn('average', col('salary') - avg(col('salary').alias('avg_sal_of_dept')).over(r)).show()



