from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def linesplit(line):
    return line.split(" ")

if __name__ == "__main__":
    # if len(sys.argv) != 2:
    #     print("Usage: hdfs_wordcount.py <directory>", file=sys.stderr)
    #     exit(-1)

    sc = SparkContext("local[4]",appName="PythonStreamingHDFSWordCount")
    ssc = StreamingContext(sc, 5)

    lines = ssc.textFileStream("input")
    counts = lines.flatMap(linesplit)\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b)
    counts.saveAsTextFiles("output/Counts")

    ssc.start()
    ssc.awaitTermination()