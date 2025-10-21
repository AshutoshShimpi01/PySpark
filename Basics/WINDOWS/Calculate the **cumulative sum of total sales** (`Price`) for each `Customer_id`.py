

#### 1. Running Total Sales Per Customer
# Calculate the **cumulative sum of total sales** (`Price`) for each `Customer_id`, ordered by `Order_date`. The output should show the lifetime spending of each customer up to every transaction.



from pyspark.sql.functions import sum, col
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType 

# 1. Join DataFrames and cast the Price column (required as it's StringType)
joined_df = sales_df.join(menu_df, 'Product_id') \
    .withColumn('Price_numeric', col('Price').cast(DoubleType()))

# 2. Define the Window Specification
# Partition: Restart the sum for each customer
# Order: Calculate the sum based on the transaction date sequence
# Frame: UNBOUNDED PRECEDING (start of partition) to CURRENT ROW (running total)
window_spec = Window.partitionBy('Customer_id').orderBy('Order_date').rowsBetween(
    Window.unboundedPreceding, 
    Window.currentRow
)

# 3. Apply the cumulative sum (SUM) over the defined window
running_total_df = joined_df.withColumn(
    'running_total_spent', 
    sum(col('Price_numeric')).over(window_spec)
)

running_total_df.select('Customer_id', 'Order_date', 'Price_numeric', 'running_total_spent').show()





2nd Way
----------

from pyspark.sql.functions import sum, col
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# 1. Define the Window Specification
# Partition by Customer_id to reset the sum for each customer.
# Order by Order_date to calculate the sum sequentially.
# Frame (rowsBetween) sets the calculation from the start of the partition to the current row.
window_spec = Window.partitionBy('Customer_id').orderBy('Order_date').rowsBetween(
    Window.unboundedPreceding,
    Window.currentRow
)

# 2. Join the data, cast Price, and apply the cumulative SUM
running_total_df = sales_df.join(menu_df, 'Product_id') \
    .withColumn('Price_numeric', col('Price').cast(DoubleType())) \
    .withColumn(
        'running_total_spent',
        sum(col('Price_numeric')).over(window_spec) # SUM applied correctly over the Window
    )

# 3. Select and display the result
running_total_df.select('Customer_id', 'Order_date', 'Price_numeric', 'running_total_spent').show()
