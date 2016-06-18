__author__ = 'Akarsh'


from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import io
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import threading
import time

#consumer key, consumer secret, access token, access secret.
ckey="0jFyRU3DZsmcnuNSfXwxX5S7O"
csecret="HQ3P8llrooT5vqJhWYxXxtRGs5FmQ2tfidsaUDrWgBfbolq5D5"
atoken="38135704-1mHF4pypfXQazuyVRqCc09Do8nfLBct1EI6ZpTy3c"
asecret="5OZ7ndGqPOGPQzJM2voGcPh03hgUFXWxtP8yu27p2zIJX"

class listener(StreamListener):
    def on_data(self, data):
        with io.open("input.txt", "a+") as file:
            file.write(data)

        return(True)

    def on_error(self, status):
        print (status)

def doTwitterStreaming():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)

    twitterStream = Stream(auth, listener())
    twitterStream.filter(track=["breaking"])

start_time = time.time() #grabs the system time
keyword_list = ['twitter'] #track list
t = threading.Thread(target=doTwitterStreaming)
try:

    t.start()

    sc = SparkContext(appName="Comp6360Project")
    ssc = StreamingContext(sc, 10)
    lines = ssc.textFileStream("input.txt")
    words = lines.filter(lambda line : line != '').flatMap(lambda line: json.loads(line)["text"].split(" "))\
                  .map(lambda x: (x, 1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    langs = lines.filter(lambda line: line != '').map(lambda line: (json.loads(line)["lang"],1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    locs = lines.filter(lambda line: line != '').map(lambda line: (json.loads(line)["user"]["location"],1))\
                  .reduceByKey(lambda a, b: a+b).map(lambda x: (x[1], x[0]))\
                  .transform(lambda rdd: rdd.sortByKey(False)).map(lambda x: (x[1], x[0]))
    words.saveAsTextFiles("output/word","")
    langs.saveAsTextFiles("output/langs","")
    locs.saveAsTextFiles("output/locs","")

    ssc.start()
    ssc.awaitTermination()

finally:
    print("end")