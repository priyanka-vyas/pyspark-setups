# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.driver.extraClassPath", r"C:\Users\info\Pyspark\mysql-connector-java-8.0.13.jar").getOrCreate()

# Read from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/office") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "mysqlpassword") \
    .load()

df.show()
df.select(["name"]).show(5)
df.filter("salary<= 20000").show(5)
df.filter("salary <= 20000").select(["name", "salary"]).show(5)
df.groupBy("dept_id").sum().show()
df.groupBy("dept_id").max().select(["dept_id", "max(salary)"]).show()
df.groupBy("dept_id").count().show()
df.groupBy("dept_id").agg({"salary": "sum"}).show()
