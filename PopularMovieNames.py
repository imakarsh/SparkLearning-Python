__author__ = 'Akarsh'

from pyspark import SparkContext, SparkConf

def loadMovieNames():
    movieNames = {}
    with open("ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("Popular Movies")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMovieNames())

moviedata = sc.textFile("C:\SparkCourse\ml-100k\\u.data")
moviecount = moviedata.map(lambda x:(int(x.split('\t')[1]),1)).reduceByKey(lambda x, y: x + y).map(lambda x:(x[1],x[0])).sortByKey(ascending=False)

movieNameCount = moviecount.map(lambda count_movieid : (nameDict.value[count_movieid[1]],count_movieid[0]))

results = movieNameCount.take(10)
for result in results:
    print (str(result[1]),str(result[0]))