
PERFECT
---------
####For each employee, calculate the **difference** between their salary and the highest salary within their department.
Display the employee name, salary, and the difference. (Hint: Use `max` as a window function).

jn_df = employees_df.join(departments_df, 'dept_id')

wind = Window.partitionBy('dept_id')

check = jn_df.withColumn('new', max('salary').over(wind) - col('salary'))

check.select('name','salary','new').show()











SAME




from pyspark.sql.functions import sum, col
from pyspark.sql.window import Window

join_df = employees_df.join(departments_df, 'dept_id')

####For each employee, calculate the **difference** between their salary and the highest salary within their department.
# Display the employee name, salary, and the difference. (Hint: Use `max` as a window function).

win = Window.partitionBy('dept_id').orderBy(col('salary').desc())

join_df.withColumn('max_sal', max(col('salary')).over(win)).withColumn('difference', col('max_sal') - col('salary')).show()

