


from pyspark.sql.window import Window
from pyspark.sql.functions import *

#3.  **Value Comparison (Lag/Lead):** For each employee, display the name and salary of the employee who earns the salary immediately **lower** than them within their department. Order the employees by salary for this comparison.

r = Window.partitionBy('dept_id').orderBy('salary')

employees_df.withColumn('lag', lag('salary').over(r)).show()
