
(comparing with impetus)

from pyspark.sql.functions import avg, col

-- identify and display all individual employees whose salary is greater than the average salary of the department they belong to.

# 1. Calculate the average salary per dept (Efficiently, without OrderBy)
dp_av = employees_df.groupBy('dept_id').agg(avg('salary').alias('dept_avg'))

# 2. Join the calculated average back to the original employee data
jn_df = employees_df.join(dp_av, on='dept_id', how='inner')

# 3. Filter employees whose salary is greater than the department average
jn_df.filter(col('salary') > col('dept_avg')).select('emp_id', 'dept_id', 'salary', 'dept_avg').show()



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


