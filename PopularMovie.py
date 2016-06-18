__author__ = 'Akarsh'

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Popular Movies")
sc = SparkContext(conf=conf)

moviedata = sc.textFile("C:\SparkCourse\ml-100k\\u.data")
moviecount = moviedata.map(lambda x:(int(x.split('\t')[1]),1)).reduceByKey(lambda x, y: x + y).map(lambda x:(x[1],x[0])).sortByKey(ascending=False)

results = moviecount.take(10)
for result in results:
    print (str(result[1]),str(result[0]))