import sys
from pyspark.sql import SparkSession, functions, types
 
spark = SparkSession.builder.appName('example 1').getOrCreate()
 
assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

cities = spark.read.csv('cities.csv', header=True, inferSchema=True)
cities.show()
cities.printSchema()

c = cities.filter(cities['area'] < 5000)
c = c.select(c['city'], c['population'])
c.show()

c.write.json('spark-output', mode='overwrite')
c.show()
print(cities.dtypes)
print(cities.schema)

some_values = cities.select(
    cities['city'],
    (cities['area'] * 1000000).alias('area_m2')
)
some_values.show()

some_values = cities.filter(cities['population'] % 2 == 1)
some_values.show()


cities = spark.read.csv('cities.csv', header=True, inferSchema=True)
c_small = cities.filter(cities['area'] < 5000)
c_droparea = c_small.select(c_small['city'], c_small['population'])
c_droparea.show()