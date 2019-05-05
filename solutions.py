from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from json import loads
from pyspark.sql.functions import to_timestamp, month, hour, col

# Read CSV Data into Pyspark
ss1 = SparkSession.builder.appName("Final_Project").getOrCreate()
df1 = ss1.read.csv("data/police-data.csv", header=True)
df1.printSchema()

# Query 1: Print the number of rows in the data frame
row_count = df1.count()
print("Count is:" + str(row_count))

# Query 2: Determine if there are duplicate rows
print("Are there duplicates?: " + str(row_count != df1.distinct().count()))


# Query 3: Write a filter to filter out only crimes that contain Suspicious Vehicle. How many?
print("Crimes commited that were S. Vehicle"+ str(df1.filter(df1["Original Crime Type Name"]=="Suspicious Vehicle").count()))

# Query 4:Write a query to filter out crimes containing Assault in the Type Name
print("Crimes committed containing Assualt:" + str(df1.filter(df1["Original Crime Type Name"].like('%Assault%')).count()))

# Query 5: Which Month has the largest number of Intoxicated Person(s)
df2 = df1.filter(df1["Original Crime Type Name"] == "Intoxicated Person")
df2 = df2.withColumn("Report Date Time", to_timestamp("Report Date","MM/dd/yyyy"))
df2.groupBy(month("Report Date Time").alias("month")).count().sort(col("count").desc()).show()
#Answer:) May

#Query 6: Which month has the largest number of crimes?
df3 = df1.withColumn("Report Date Time", to_timestamp("Report Date","MM/dd/yyyy"))
df3.groupBy(month("Report Date Time").alias("month")).count().sort(col("count").desc()).show()
#Answer:) October

#Query 7: Which hour of the day has the most number of offences?
df4 = df1.withColumn("Call Time Stamp", to_timestamp("Call Time","HH:mm"))
df4.groupBy(hour("Call Time Stamp").alias("hour")).count().sort(col("count").desc()).show()
#Answer:) 17 (aka 5 o clock)