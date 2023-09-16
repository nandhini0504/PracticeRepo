from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
df = spark.createDataFrame(data=data, schema = columns)

data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

#Change DataType using PySpark withColumn()  #By using PySpark withColumn() on a DataFrame, we can cast or change the data type of a column.
# also be used to change the value of an existing column.


df.withColumn("salary",col("salary").cast("Integer")).show()

# Update The Value of an Existing Column
#In order to change the value, pass an existing column name as a first argument and
# a value to be assigned as a second argument to the withColumn() function

df.withColumn("salary",col("salary")*100).show()

#Create a Column from an Existing
#To add/create a new column, specify the first argument with a name you want your new column to be
# and use the second argument to assign a value by applying an operation on an existing column
df.withColumn("CopiedColumn",col("salary")* -1).show()

# Add a New Column using withColumn()
# PySpark lit() function is used to add a constant value to a DataFrame column
df.withColumn("Country", lit("USA")).show()
df.withColumn("Country", lit("USA")) \
  .withColumn("anotherColumn",lit("anotherValue")) \
  .show()

#Rename Column Name


df.withColumnRenamed("gender","sex") \
  .show(truncate=False)

#Drop Column From PySpark DataFrame

df.drop("salary") \
  .show()


