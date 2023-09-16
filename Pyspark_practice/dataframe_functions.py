# show() function on DataFrame prints the result of DataFrame in a table format. By default it show only 20 rows
from pyspark.sql.functions import col

#Select Single & Multiple Columns From PySpark


df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

#By using col() function
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

#Select columns by regular expression
df.select(df.colRegex("`^.*name*`")).show()

#Select All Columns From List

# Select All columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()


#Select Columns by Index

#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

#Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)


# PySpark Collect() – Retrieve data from DataFrame
#collect() function of the RDD/DataFrame is an action operation that returns all elements of the DataFrame
#to spark driver program and also learned it’s not a good practice to use it on the bigger dataset.


dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]

deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)

dataCollect = deptDF.collect()

print(dataCollect)

dataCollect2 = deptDF.select("dept_name").collect()
print(dataCollect2)

for row in dataCollect:                      #This line initiates a loop that iterates through each row in the dataCollect list.
    print(row['dept_name'] + "," +str(row['dept_id']))
