

from pyspark.sql.window import Window
from pyspark.sql.functions import *

#**Relative Salary Comparison (Avg):** Calculate a new column showing the difference between each employee's salary and the average salary of their respective department.

r = Window.partitionBy('dept_id')

employees_df.withColumn('average', col('salary') - avg(col('salary').alias('avg_sal_of_dept')).over(r)).show()




2nd way




from pyspark.sql.functions import *
from pyspark.sql.window import *

jn = employees_df.join(departments_df, 'dept_id')

win = Window.partitionBy('dept_id')

check = jn.withColumn('average_salary', avg('salary').over(win))

check.withColumn('difference_betn_salary', col('average_salary') - col('salary')).show()
