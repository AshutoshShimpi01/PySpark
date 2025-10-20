
Task:
Given employee transaction data with - Empld, Date, Category, Amount
Find the Top 2 Employees for Each Month based on total
transaction amount ($
Sounds easy? Try handling aggregations, date transformations, and ranking efficiently using window functions in PySpark! C
Would you solve this using row_number) or dense_rank)?




from pyspark.sql.window import Window
from pyspark.sql.functions import *

monthly_spend_df = transaction_df.withColumn('month', date_format(col('date'), 'yyyy-mm'))
                   .groupBy('emp_id','month').agg(sum('amount').alias('total_spent'))

window_spec = Window.partitionBy('month').orderBy(col('total_spent').desc())

ranked_df = monthly_spend_df.withColumn('rk', dense_rank().over(window_spec))
ranked_df.filter(col('rk') <= 2).show() 













# Assuming 'transactions_df' is the input DataFrame

# 1. Aggregation and Date Transformation
monthly_spending_df = transactions_df.withColumn(
    "YearMonth",
    date_format(col("Date"), "yyyy-MM") # Create a 'yyyy-MM' column
).groupBy("EmpId", "YearMonth").agg(
    sum("Amount").alias("TotalSpending") # Calculate total spending per employee per month
)

# 2. Window Definition and Ranking
window_spec = Window.partitionBy("YearMonth").orderBy(col("TotalSpending").desc())

ranked_df = monthly_spending_df.withColumn(
    "MonthlyRank",
    dense_rank().over(window_spec)
)

# 3. Filtering for Top 2
top_2_employees_monthly = ranked_df.filter(col("MonthlyRank") <= 2)

# top_2_employees_monthly.show()
