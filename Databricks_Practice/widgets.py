%md ####Read Data
%sql
CREATE TABLE user USING CSV OPTIONS (path "/FileStore/tables/user.csv", header "true")
%sql
select * from user
%sql
select location, user_id from user WHERE location="usa"
df1 = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/nandhini.k@diggibyte.com/product.csv")
display(df1)

dbutils.fs.help('ls')
dbutils.data.summarize(df1)
dbutils.fs.help("cp")
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import *

# Initialize SparkSession
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Define your user data and schema
user_data = [
    ({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", 3000),
    ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", 20000),
    ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", 3000),
    ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "03011998", "F", 11000),
    ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "04101998", "F", 10000)
]

user_schema = StructType([
    StructField('name', StructType([
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True)])),
    StructField("dob", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("salary", IntegerType(), True)
])

# Create a DataFrame
user_df = spark.createDataFrame(data=user_data, schema=user_schema)

# Show the DataFrame
user_df.show()

select_col_df=user_df.select(col("name.firstname"), col("name.lastname"), col("salary"))
select_col_df.show()
country_df= user_df.withColumn("country",lit("India"))
    department_df=country_df.withColumn("department",lit("sales"))
    age_df=department_df.withColumn("age",
                when(col("name.firstname") == "James", lit("30")).
                when(col("name.firstname")=="Michael",lit("23")).
                when(col("name.firstname") == "Robert", lit("25")).
                when(col("name.firstname") == "Maria", lit("20")).
                otherwise(lit("20")))
country_df= user_df.withColumn("country",lit("India"))
department_df=country_df.withColumn("department",lit("sales"))
age_df=department_df.withColumn("age",
        when(col("name.firstname") == "James", lit("30")).
        when(col("name.firstname") == "Michael",lit("23")).
        when(col("name.firstname") == "Robert", lit("25")).
        when(col("name.firstname") == "Maria", lit("20")).
        otherwise(lit("20")))
age_df.show()
dbutils.fs.help()
dbutils.widgets.combobox(name='fruitsCB',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits ComboBox')
dbutils.widgets.dropdown(name='fruitsDD',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits dropdown')
dbutils.widgets.multiselect(name='fruitsMS',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits Multiselect')
dbutils.widgets.text(name='fruitsTB',defaultValue='apple',label='Fruits Textbox')
dbutils.widgets.get('fruitsMS')
dbutils.widgets.getArgument('fruitsMS1','error:this widget is not available')

