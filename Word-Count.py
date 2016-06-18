#count the number of unique words
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/SparkCourse/book.txt")
words = lines.flatMap(lambda x: x.split())
words = words.map(lambda x:str(x.encode('ascii', 'ignore'),'utf-8'))
wordCounts = words.countByValue()

for word, count in wordCounts.items():
#    cleanWord = word.encode('ascii', 'ignore') #make sure we have UTF8 or unicode, ignore conversion errors
#    if (cleanWord):
        print(word, count)

#!spark-submit Word-Count.py
