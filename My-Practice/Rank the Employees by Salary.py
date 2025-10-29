
####For each department, **rank** employees based on their salary from highest to lowest.
Display the employee name, salary, department name, and the salary rank. Use the `rank` function.



from pyspark.sql.window import Window
from pyspark.sql.functions import *


####For each department, **rank** employees based on their salary from highest to lowest. Display the employee name, salary, department name, and the salary rank. Use the `rank` function.

jn_df = employees_df.join(departments_df, 'dept_id')

win = Window.partitionBy('dept_id').orderBy(col('salary').desc())

final_df = jn_df.withColumn('rk', rank().over(win))

final_df.select('name','salary','dept_name','rk').show()

