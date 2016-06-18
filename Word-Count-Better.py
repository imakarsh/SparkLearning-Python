# count distinct words only
import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

lines = sc.textFile("C:/SparkCourse/book.txt")
words = lines.flatMap(normalizeWords)
words = words.map(lambda x:str(x.encode('ascii', 'ignore'),'utf-8'))

wordCounts = words.countByValue()

for word, count in list(wordCounts.items()):
#    cleanWord = word.encode('ascii', 'ignore')
#    if (cleanWord):
        print(word, count)

# !spark-submit Word-Count-Better.py
