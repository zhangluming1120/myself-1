import sys
from pyspark.sql import SparkSession, functions, types, Row
import string, re

spark = SparkSession.builder.appName('correlate logs').getOrCreate()

assert sys.version_info >= (3, 4) # make sure we have Python 3.4+
assert spark.version >= '2.1' # make sure we have Spark 2.1+
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, explode
from pyspark.sql.functions import lower


wordbreak = r'[%s\s]+' % (re.escape(string.punctuation),)  # regex that matches spaces and/or punctuation



line_re = re.compile(r"^(\S+)")


def line_to_row(line):
    """
    Take a logfile line and return a Row object with hostname and bytes transferred. Return None if regex doesn't match.
    """
    m = line_re.match(line)
    if m:
        # TODO
        return Row(word= (m.group(1)).lower()) 
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


def main(in_directory, out_directory):
    # spark.createDataFrame(create_row_rdd(in_directory))
    text = spark.read.text(in_directory)
    #text = text.select(split(text, wordbreak))
    #text = split(text, wordbreak)
    word = text.select(split(text['value'], wordbreak).alias("word_list"))
    word = word.select(explode(word["word_list"]).alias("word_temp"))


    word = word.filter(word['word_temp'] != '')
    #word = word.lower(word['word_temp'])
    word = word.select(lower(word["word_temp"]).alias("word_clean"))

    #list = word['word_temp']


    #word = word = word.select("word_list")
    #word = word.select(explode(word.select('word_list')) )
    #df.select(split(df.s, '[0-9]+').alias('s')).collect()

    #word = word.sort("word_clean", ascending=False)
    word = word.groupBy('word_clean').agg(
        functions.count(word['word_clean']).alias("count"))

    word = word.sort("count", ascending=False)
    

    #logs = logs.groupBy('hostname')

    word.write.csv(out_directory + '-word_count', mode='overwrite')

    #print( (n, x, x2, y, y2, xy) )
    #return
    # TODO: calculate r.





if __name__=='__main__':
    in_directory = sys.argv[1]
    out_directory = sys.argv[2]
    main(in_directory, out_directory)
