

from pyspark.sql.functions import *
from pyspark.sql.window import *

# Find the customers whose place the orders in consecutive Location 


win = Window.partitionBy('Customer_id').orderBy('order_date')

df_with_prev = jn.withColumn('previous_location', lag(col('Source_order'), 1).over(win))

df_consecutive_check = df_with_prev.withColumn('is_consecutive_location', when(col('Source_order') == col('previous_location'), True).otherwise(False))

df_consecutive_check.filter(col('is_consecutive_location') == True).select('Customer_id', 'order_date', 'Source_order', 'previous_location').show()

