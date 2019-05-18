import sys
from pyspark.sql import SparkSession, functions, types

spark = SparkSession.builder.appName('reddit relative scores').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+

schema = types.StructType([ # commented-out fields won't be read
    #types.StructField('archived', types.BooleanType(), False),
    types.StructField('author', types.StringType(), False),
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
    types.StructField('score', types.LongType(), False),
    #types.StructField('score_hidden', types.BooleanType(), False),
    types.StructField('subreddit', types.StringType(), False),
    #types.StructField('subreddit_id', types.StringType(), False),
    #types.StructField('ups', types.LongType(), False),
])


def main(in_directory, out_directory):
    comments = spark.read.json(in_directory, schema=schema).cache()


    #comments.show(); return
    # TODO: calculate averages, sort by subreddit. Sort by average score and output that too.
    comments2 = comments.groupBy('subreddit').agg(
        functions.avg(comments['score']).alias('average score'))
    #Exclude any subreddits with average score ≤0.
    comments2 = comments2.filter(comments2['average score'] > 0)
    #functions.broadcast(comments2)
    #comments.show(); return
    #comments2.show();return
    df_joined = comments.join(comments2, ['subreddit'])
    #df_joined.show(); return
    df_joined = df_joined.withColumn('rel_score',
                        df_joined['score']/ df_joined['average score']).cache()
    #df_joined.show(); return
    
    df_max =  df_joined.groupBy('subreddit').agg(
            functions.max(df_joined['rel_score']).alias('rel_score'))
    #functions.broadcast(df_max)
    #df_max.show(); return
    #df_joined.show(); return
    commonlist = ["subreddit", 'rel_score']
    df_max = df_max.join(df_joined, commonlist)
    #df_max.show(); return
    df_max = df_max.drop('average score', 'score')
    #df_max.show(); return
    best_author = df_max.select('subreddit','author','rel_score')
    #best_author.show()
    best_author.write.json(out_directory, mode='overwrite')


if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
