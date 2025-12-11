

from pyspark.sql.functions import *
from pyspark.sql.window import *

## Question 1 : Find the top three customers by total order value 


sales_df.join(menu_df, 'Product_id').groupBy('Customer_id').agg(sum('Price').alias('total')).orderBy(col('total').desc()).limit(3).show()

