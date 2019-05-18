import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit averages').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+


schema = types.StructType([ # commented-out fields won't be read
    #types.StructField('archived', types.BooleanType(), False),
    #types.StructField('author', types.StringType(), False),
    #types.StructField('author_flair_css_class', types.StringType(), False),
    #types.StructField('author_flair_text', types.StringType(), False),
    #types.StructField('body', types.StringType(), False),
    #types.StructField('controversiality', types.LongType(), False),
    #types.StructField('created_utc', types.StringType(), False),
    #types.StructField('distinguished', types.StringType(), False),
    #types.StructField('downs', types.LongType(), False),
    #types.StructField('edited', types.StringType(), False),
    #types.StructField('gilded', types.LongType(), False),
    #types.StructField('id', types.StringType(), False),
    #types.StructField('link_id', types.StringType(), False),
    #types.StructField('name', types.StringType(), False),
    #types.StructField('parent_id', types.StringType(), True),
    #types.StructField('retrieved_on', types.LongType(), False),
    #types.StructField('score', types.LongType(), False),
    #types.StructField('score_hidden', types.BooleanType(), False),

    types.StructField('language', types.StringType(), False),
    types.StructField('titles', types.StringType(), False),
    types.StructField('times_requested', types.LongType(), False),
    types.StructField('num_of_bytes', types.LongType(), False),

    #types.StructField('subreddit_id', types.StringType(), False),
    #types.StructField('ups', types.LongType(), False),
])

import ntpath
ntpath.basename("a/b/c")

def path_leaf(path):
    head, tail = ntpath.split(path)
    return tail

def extract(filename):
    return filename[11:22]


complicated_udf = functions.udf(path_leaf,
                returnType=types.StringType())



complicated_udf2 = functions.udf(extract,
                returnType=types.StringType())

@functions.udf(returnType=types.StringType())
def python_logic(path):
    head, tail = ntpath.split(path)
    return tail


def main(in_directory, out_directory):
    #we're interested in "en"only. 
    df = spark.read.csv(in_directory, schema = schema ,sep=' ').withColumn('path', functions.input_file_name())
    #df.show(); return
    #df.printSchema(); return
    df = df.withColumn('filename',
        python_logic(df['path']))
    df = df.drop('path')
    #df.show(); return
    df = df.withColumn('time',
        complicated_udf2(df['filename'])) 
    #df.show(); return
    df = df.drop('filename')
    #df.show(); return
    df = df.drop('num_of_bytes')
    #df.show(); return
    df = df.filter(df['language'] == 'en')
    #df.show(); return
    df = df.drop('language')
    df = df.filter(df['titles'] != ('Main_Page'))
    #df.show(); return
    df = df.filter(~df['titles'].contains('Special:'))
    df = df.cache()
    df_max = df.groupBy('time').agg(
        functions.max(df['times_requested']).alias('times_requested'))
    #df.show(); return
    #df_max.show(); return
    df_joined = df_max.join(df, ['time', 'times_requested'])
    df_joined = df_joined.sort('time', ascending = True)
    df_joined = df_joined.select('time','titles','times_requested')
    #df_joined = df_joined.count();
    df_joined.show(); return
    df_joined.write.csv(out_directory , mode='overwrite')

if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
