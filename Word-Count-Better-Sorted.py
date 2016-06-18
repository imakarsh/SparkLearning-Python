#Word count without punctuations and sorted
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/SparkCourse/book.txt")
words = lines.flatMap(normalizeWords)
words = words.map(lambda x:str(x.encode('ascii', 'ignore'),'utf-8'))

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y) #put 1 next to word, add all 1s
wordCountsSorted = wordCounts.map(lambda x_y: (x_y[1],x_y[0])).sortByKey() #swap count and words and sort by count

results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1]#.encode('ascii', 'ignore')
    if (word):
        print(word + ":\t\t" + count)

# !spark-submit Word-Count-Better-Sorted.py
