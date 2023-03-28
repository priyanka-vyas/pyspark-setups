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

# Read data from multiple MySQL tables
employee_df = spark.read.jdbc(url=url,table="(select * from employee)e",properties=prop)
department_df = spark.read.jdbc(url=url,table="(select * from department)d",properties=prop)
salary_df = spark.read.jdbc(url=url,table="(select * from manager)m",properties=prop)
employee_df.show()
department_df.show()
salary_df.show()
# # Perform transformations on the data
# # ...

# # Write data back to MySQL
# employee_df.write.jdbc(url=url,table="employee",mode="overwrite",properties=prop)
# department_df.write.jdbc(url=url,table="department",mode="overwrite",properties=prop)
# salary_df.write.jdbc(url=url,table="salary",mode="overwrite",properties=prop)