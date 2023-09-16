import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col,array_contains
spark= SparkSession.builder.appName("filter_condition").getOrCreate()


arrayStructureData = [
    (("James", "", "Smith"), ["Java", "Scala", "C++"], "OH", "M"),
    (("Anna", "Rose", ""), ["Spark", "Java", "C++"], "NY", "F"),
    (("Julia", "", "Williams"), ["CSharp", "VB"], "OH", "F"),
    (("Maria", "Anne", "Jones"), ["CSharp", "VB"], "NY", "M"),
    (("Jen", "Mary", "Brown"), ["CSharp", "VB"], "NY", "M"),
    (("Mike", "Mary", "Williams"), ["Python", "VB"], "OH", "M")
]

arrayStructureSchema = StructType([
    StructField('name', StructType([
        StructField('firstname', StringType(), True),
        StructField('middlename', StringType(), True),
        StructField('lastname', StringType(), True)
    ])),
    StructField('languages', ArrayType(StringType()), True),
    StructField('state', StringType(), True),
    StructField('gender', StringType(), True)
])

df = spark.createDataFrame(data=arrayStructureData, schema=arrayStructureSchema)
df.printSchema()
df.show(truncate=False)

#DataFrame filter() with Column Condition

df.filter(df.state == "OH") \
    .show(truncate=False)


#Using SQL col() function  (This one and above code both gives same result)
#from pyspark.sql.functions import col
df.filter(col("state") == "OH") \
    .show(truncate=False)

#DataFrame filter() with SQL Expression


#For not equal
# df.filter("gender != 'M'").show()
# df.filter("gender <> 'M'").show()

# Using SQL Expression
df.filter("gender  == 'M'") \
    .show(truncate=False)
#PySpark Filter with Multiple Conditions
df.filter((df.state == "OH") & (df.gender == "M")) \
    .show(truncate=False)

#Filter Based on List Values

#Filter IS IN List values              If you have a list of elements and you wanted to filter that is not in the list or in the list, use isin() function of Column class
li=["OH","CA","DE"]
df.filter(df.state.isin(li)).show()

# Filter NOT IS IN List values
#These show all records with NY (NY is not part of the list)
df.filter(~df.state.isin(li)).show()
df.filter(df.state.isin(li)==False).show()

#Filter Based on Starts With, Ends With, Contains


# Using startswith
df.filter(df.state.startswith("N")).show()
#+--------------------+------------------+-----+------+
#|                name|         languages|state|gender|
#+--------------------+------------------+-----+------+
#|      [Anna, Rose, ]|[Spark, Java, C++]|   NY|     F|
#|[Maria, Anne, Jones]|      [CSharp, VB]|   NY|     M|
#|  [Jen, Mary, Brown]|      [CSharp, VB]|   NY|     M|
#+--------------------+------------------+-----+------+

#using endswith
df.filter(df.state.endswith("H")).show()

#contains
df.filter(df.state.contains("H")).show()
# PySpark Filter like and rlike


data2 = [(2,"Michael Rose"),(3,"Robert Williams"),
     (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

# like - SQL LIKE pattern
df2.filter(df2.name.like("%rose%")).show()
#+---+----------+
#| id|      name|
#+---+----------+
#|  5|Rames rose|
#+---+----------+

# rlike - SQL RLIKE pattern (LIKE with Regex)
#This check case insensitive
df2.filter(df2.name.rlike("(?i)^*rose$")).show()
#+---+------------+
#| id|        name|
#+---+------------+
#|  2|Michael Rose|
#|  4|  Rames Rose|
#|  5|  Rames rose|

#Filter on an Array column

from pyspark.sql.functions import array_contains
df.filter(array_contains(df.languages,"Java")) \
    .show(truncate=False)

#Filtering on Nested Struct columns

df.filter(df.name.lastname == "Williams") \
    .show(truncate=False)
