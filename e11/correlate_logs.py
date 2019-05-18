import sys
from pyspark.sql import SparkSession, functions, types, Row
import re

spark = SparkSession.builder.appName('correlate logs').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+
import math


line_re = re.compile(r"^(\S+) - - \[\S+ [+-]\d+\] \"[A-Z]+ \S+ HTTP/\d\.\d\" \d+ (\d+)$")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred. Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        return Row(hostname=m.group(1), bytes=m.group(2)) 
    else:
        return None

def not_none(row):
    """
    Is this None? Hint: .filter() with it.
    """
    return row is not None


def create_row_rdd(in_directory):
    log_lines = spark.sparkContext.textFile(in_directory)
    # TODO: return an RDD of Row() object
    rows = log_lines.map(line_to_row)
    rows = rows.filter(not_none)
    return rows


def complicated_function(a, b):
    return a + 2*b  # pretend this is complex work.

complicated_udf = functions.udf(complicated_function,
                        returnType=types.IntegerType())

def complicated_function2(a, b):
    return a + 2*b*math.log2(b)


complicated_udf2 = functions.udf(complicated_function2,
                        returnType=types.IntegerType())


def main(in_directory):
    #import pandas as pd
    #pd_data = pd.DataFrame([[1,2], [3,4], [5,6]],
    #                       columns=['width', 'height'])
    #data = spark.createDataFrame(pd_data).collect()
    #data.show()
    #print (type(data)); return



    #ints = spark.range(10000)
    #result = ints.select(
    #    ints['id'],
    #    complicated_udf2(ints['id'], ints['id']+1).alias('res')
    #)
    #result.show(5); return
    row_object = create_row_rdd(in_directory)
    #from pprint import pprint
    #pprint(row_object.take(20)); return
    logs = spark.createDataFrame(row_object)
    

    #logs.show(); return
    logs = logs.groupBy('hostname').agg(
            functions.count('bytes').alias('count_requests'),
            functions.sum('bytes').alias('sum_request_bytes'))
    #logs.show(); return
    logs = logs.withColumn( '1', functions.lit(1.0) )
    logs = logs.withColumn( 'x', logs['count_requests'] )
    #logs.show(); return
    logs = logs.withColumn( 'x2', logs['x'] * logs['x'])
    logs = logs.withColumn( 'y', logs['sum_request_bytes'] )
    logs = logs.withColumn( 'y2', logs['y'] * logs['y'] )
    logs = logs.withColumn( 'xy', logs['x'] * logs['y'] )
    #logs.show(); return
    #logs.groupby('x')
    #groupby an aggregate
    sums = logs.select( [ functions.sum( '1' ).alias( '1' ),
                          functions.sum( 'x' ).alias( 'x' ),
                          functions.sum( 'x2' ).alias( 'x2' ),
                          functions.sum( 'y' ).alias( 'y' ),
                          functions.sum( 'y2' ).alias( 'y2' ),
                          functions.sum( 'xy' ).alias( 'xy' ) ] )
    ls = sums.collect()
    #sums.show(); return
    n = ls[0]['1']
    x = sums.collect()[0]['x']
    x2 = sums.collect()[0]['x2']
    y = sums.collect()[0]['y']
    y2 = sums.collect()[0]['y2']
    xy = sums.collect()[0]['xy']
    #print( (n, x, x2, y, y2, xy) )
    #return
    # TODO: calculate r.




    r = ( n * xy - x * y ) / ( ( ( n * x2 - x ** 2 ) ** 0.5 ) * ( ( n * y2 - y ** 2 ) ** 0.5 ) )

    #r = 0 # TODO: it isn't zero.
    print("r = %g\nr^2 = %g" % (r, r**2))


if __name__=='__main__':
    in_directory = sys.argv[1]
    main(in_directory)
