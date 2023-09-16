import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType,IntegerType
from pyspark.sql.functions import col,struct,when  #col for column references, struct for creating a struct type, and when for conditional expressions.


spark = SparkSession.builder.appName('Example1').getOrCreate()

# access the Column from DataFrame by multiple ways

data=[("James",23),("Ann",40)]
df=spark.createDataFrame(data).toDF("name.fname","gender")
df.printSchema()
#root
# |-- name.fname: string (nullable = true)
# |-- gender: long (nullable = true)

# Using DataFrame object (df)
df.select(df.gender).show()
df.select(df["gender"]).show()
#Accessing column name with dot (with backticks)
df.select(df["`name.fname`"]).show()

#Using SQL col() function
from pyspark.sql.functions import col
df.select(col("gender")).show()
#Accessing column name with dot (with backticks)
df.select(col("`name.fname`")).show()
