 #Imports
from pyspark.sql import SparkSession
from utils.test import load_yaml


connection=load_yaml()

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.driver.extraClassPath", r"C:\Users\info\Pyspark\mysql-connector-java-8.0.13.jar").getOrCreate()

prop={
    "user":connection.get("user"),
    "password":connection.get("password"),
    "driver":connection.get("driver"),
    "numPartitions":connection.get("numPartitions")
}

# print(prop)

# prop={
#     "user":"root",
#     "password":"mysqlpassword",
#     "driver":"com.mysql.jdbc.Driver",
#     "numPartitions":"4"
# }
employee_df = spark.read.jdbc(url=connection.get("url"),table="(select * from employee)e",properties=prop)

employee_df.show()


