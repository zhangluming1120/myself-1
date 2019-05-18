import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('weather ETL').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
])


def main(in_directory, out_directory):
    weather = spark.read.csv(in_directory, schema=observation_schema)

    # TODO: finish here.
    #weather.show(); return
    w = weather.filter(weather['qflag'].isNull())

    #w.show(); return
    w2 = w.filter(w['station'].startswith('CA'))
    #w2.show(); return
    w3 = w2.filter(w2['observation'] =='TMAX')
    w3.show()
    w4 = w3.withColumn('TMAX', w3['value']/10)
    w4.show()
    w5 = w4.select(w4['station'], w4['date'], w4['TMAX'])
    w5.show(); 

    cleaned_data = w5
    cleaned_data.write.json(out_directory, compression='gzip', mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
