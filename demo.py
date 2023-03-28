from pyspark.sql import SparkSession
from pyspark.sql.functions import concat
#Creating a Spark Session
spark = SparkSession.builder.appName('Introduction to Spark').getOrCreate()
print(spark)
#Loading dataset to PySpark
df = spark.read.csv(r'C:\Users\info\Pyspark\employees.csv',header = True, inferSchema = True)
# df.show()
# #Print Data Types
# df.printSchema()
# print(df.columns)
# df.head(3)
# #2. Exploring DataFrame
# #Selecting columns

df.select("EMPLOYEE_ID").show(5)

# df.select(["FIRST_NAME", "SALARY"]).show(5)
# df.select(df[1], df[2]).show(5)
# #Describe Data Frame
# df.describe().show()
# #Generating a New Column
# # df = df.withColumn("FULL_NAME", (df["FIRST_NAME"]+df["LAST_NAME"]))
# # df = df.withColumn("full_name", concat(df[FIRST_NAME], " ", df[LAST_NAME]))
# # df.show(5)

df.select(concat(df.FIRST_NAME, " ", df.LAST_NAME).alias("FULL_NAME")).show()
df.show()