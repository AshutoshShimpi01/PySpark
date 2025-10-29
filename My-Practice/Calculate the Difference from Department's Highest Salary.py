


####For each employee, calculate the **difference** between their salary and the highest salary within their department.
Display the employee name, salary, and the difference. (Hint: Use `max` as a window function).

jn_df = employees_df.join(departments_df, 'dept_id')

wind = Window.partitionBy('dept_id')

check = jn_df.withColumn('new', max('salary').over(wind) - col('salary'))

check.select('name','salary','new').show()
