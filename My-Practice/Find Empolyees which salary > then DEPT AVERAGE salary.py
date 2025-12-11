
(comparing with impetus)

from pyspark.sql.functions import avg, col

-- identify and display all individual employees whose salary is greater than the average salary of the department they belong to.

# 1. Calculate the average salary per dept (Efficiently, without OrderBy)
dp_av = employees_df.groupBy('dept_id').agg(avg('salary').alias('dept_avg'))

# 2. Join the calculated average back to the original employee data
jn_df = employees_df.join(dp_av, on='dept_id', how='inner')

# 3. Filter employees whose salary is greater than the department average
jn_df.filter(col('salary') > col('dept_avg')).select('emp_id', 'dept_id', 'salary', 'dept_avg').show()
