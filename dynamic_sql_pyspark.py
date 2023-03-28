
# Imports
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.driver.extraClassPath", r"C:\Users\info\Pyspark\mysql-connector-java-8.0.13.jar").getOrCreate()

# Define connection properties
url="jdbc:mysql://localhost:3306/office"
prop={
    "user":"root",
    "password":"mysqlpassword",
    "driver":"com.mysql.jdbc.Driver",
    "numPartitions":"4"
}

# Define list of table names
tables = ["employee", "department", "manager"]

# Loop through tables
for table_name in tables:
    table_df = spark.read.jdbc(url=url,table=f"(select * from {table_name}){table_name[0]}",properties=prop)
    table_df.show()
#
    


