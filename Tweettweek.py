__author__ = 'Akarsh'

from pyspark import SparkContext
import json

sc = SparkContext(appName="Comp6360Project")
lines = sc.textFile("input.txt")
#.filter(lambda line : line != '')
words = lines.filter(lambda line: line != '').flatMap(lambda line: json.loads(line)["text"].split(" "))\
              .map(lambda x: (x, 1))\
              .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
              .sortByKey(False).map(lambda x: (x[1], x[0]))
langs = lines.filter(lambda line: line != '').map(lambda line: (json.loads(line)["lang"],1))\
              .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
              .sortByKey(False).map(lambda x: (x[1], x[0]))
locs = lines.filter(lambda line: line != '').map(lambda line: (json.loads(line)["user"]["location"],1))\
              .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
              .sortByKey(False).map(lambda x: (x[1], x[0]))
words.saveAsTextFile("output/word","")
langs.saveAsTextFile("output/langs","")
locs.saveAsTextFile("output/locs","")