# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.driver.extraClassPath", r"C:\Users\info\Pyspark\mysql-connector-java-8.0.13.jar").getOrCreate()
url="jdbc:mysql://localhost:3306/office"
table="""(select * from employee)e"""
prop={
    "user":"root",
    "password":"mysqlpassword",
    "driver":"com.mysql.jdbc.Driver",
    "numPartitions":"4"
}
# Read from MySQL Table
df = spark.read.jdbc(url=url,table=table,properties=prop)

df.show()