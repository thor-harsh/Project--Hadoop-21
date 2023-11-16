from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark=SparkSession.builder.appName('SparkSQL').getOrCreate()

lines=spark.read.option('header','true').option('inferSchema','true')\
    .csv('fakefriends-header.csv')

#print the infer schema
print('Here is your inferred schema')
lines.printSchema()

#Selecting only age and friends columns
people=lines.select('age','friends')

print('Here is your avg no of friends by age')
people.groupby('age').avg('friends').show()

#Sorted
people.groupby('age').avg('friends').sort('age').show()

#Formatted more nicely
people.groupby('age').agg(func.round(func.avg('friends'),2)).sort('age').show()

#Now add alias also
people.groupby('age').agg(func.round(func.avg('friends'),2).alias('friends_avg')).sort('age').show()

#Stop the Spark Session
spark.stop()