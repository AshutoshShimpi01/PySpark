
(comparing with impetus)

from pyspark.sql.functions import avg, col

-- identify and display all individual employees whose salary is greater than the average salary of the department they belong to.



from pyspark.sql.functions import *
from pyspark.sql.window import *


#identify and display all individual employees whose salary is greater than the average salary of the department they belong to.


dpt_av = employees_df.groupBy('dept_id').agg(avg('salary').alias('d_av'))

jn = employees_df.join(dpt_av, 'dept_id')

jn.filter(col('salary') > col('d_av')).show()





# 




SAME IN 2 Lines

d_avg = employees_df.groupBy('dept_id').agg(avg('salary').alias('dept_av'))

jn = employees_df.join(d_avg, 'dept_id').filter(col('salary') > col('dept_av')).select('emp_id', 'dept_id', 'salary', 'dept_av').show()








SAME USING WINDOW FUNCTIONS


steps -  
join datafraes --> write window --> create new dataframe to perform aggregration --> apply finter condition inside question


jn = employees_df.join(departments_df, 'dept_id')
win = Window.partitionBy('dept_id').orderBy(col('salary'))
check = jn.withColumn('dept_av', avg('salary').over(win))
check.filter(col('salary') > col('dept_av')).show()











.


