Useful PySpark Functions (like size())



Whenever you use functions like col, lit, when, split, size, etc. — make sure to import them first:
from pyspark.sql.functions import col, lit, when, split, size, upper, length, round


1. size() → 📏 Count elements in an array
 
from pyspark.sql.functions import size, split

df.withColumn("marks_count", size(split(col("marks"), ",")))



2. length() → 🔡 Length of a string
 
from pyspark.sql.functions import length

df.withColumn("name_length", length(col("name")))



3. split() → ✂️ Split a string into an array
 
from pyspark.sql.functions import split

df.withColumn("marks_array", split(col("marks"), ","))



4. concat() → 🔗 Concatenate multiple columns
 
from pyspark.sql.functions import concat, lit

df.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))



5. substring() → 🧵 Extract part of a string
 
from pyspark.sql.functions import substring

df.withColumn("first_3_letters", substring(col("name"), 1, 3))



6. upper() / lower() → 🔠 Case conversion
 
from pyspark.sql.functions import upper, lower

df.withColumn("name_upper", upper(col("name")))
df.withColumn("name_lower", lower(col("name")))



7. trim() → ✂️ Remove whitespace
 
from pyspark.sql.functions import trim

df.withColumn("clean_name", trim(col("name")))



8. array_contains() → 🔍 Check if array contains a value
 
from pyspark.sql.functions import array_contains, split

df.withColumn("has_100", array_contains(split(col("marks"), ","), "100"))



9. regexp_replace() → 🔁 Replace using regex
 
from pyspark.sql.functions import regexp_replace



df.withColumn("clean_marks", regexp_replace(col("marks"), ",", "|"))



10. round() / floor() / ceil() → 🔢 Numeric operations
 
from pyspark.sql.functions import round, floor, ceil

df.withColumn("rounded_price", round(col("price"), 2))



11. when() / otherwise() → 🧠 Conditional logic
 
from pyspark.sql.functions import when

df.withColumn("grade", when(col("marks") > 90, "A").otherwise("B"))



12. year() / month() / dayofmonth() → 📅 Date extract
 
from pyspark.sql.functions import year, month

df.withColumn("birth_year", year(col("dob")))
