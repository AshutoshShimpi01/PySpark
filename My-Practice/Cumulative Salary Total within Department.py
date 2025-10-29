

####Calculate the **running total (cumulative sum)** of salaries for employees within each department, 
ordered by their hire date.






from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum

# Using 'salary' for ordering since 'hire_date' is missing, but this changes the problem's intent.
wind_running_sum = Window.partitionBy('dept_id').orderBy(col('salary')).rowsBetween(Window.unboundedPreceding, Window.currentRow)


final_df = employees_df.withColumn(
    'running_salary_total',
    sum('salary').over(wind_running_sum)
)

final_df.select('name', 'dept_id', 'salary', 'running_salary_total').show()
# Note: 'hire_date' is removed from select list as it doesn't exist.
