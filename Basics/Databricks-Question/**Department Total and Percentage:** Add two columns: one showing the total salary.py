

#5.  **Department Total and Percentage:** Add two columns: one showing the total salary expenditure for the employee's department, 
# and another showing the employee's salary as a percentage of that department total.

my Query

from pyspark.sql.functions import *
from pyspark.sql.window import *

win = Window.partitionBy('dept_id')

dept_total = employees_df.withColumn('total_salary', sum('salary').over(win))
final = dept_total.withColumn('perc_of_dept_total', col('salary') / col('total_salary') * 100).show()
