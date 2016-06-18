__author__ = 'Akarsh'

from pyspark import SparkConf, SparkContext

def custsale(line):
    res = line.split(",")
    return (int(res[0]),float(res[2]))

conf = SparkConf().setMaster("local").setAppName("CustomerSpent")
sc = SparkContext(conf=conf)

sales = sc.textFile("C://SparkCourse/customer-orders.csv")
customersales = sales.map(custsale)
custSpent = customersales.reduceByKey(lambda x, y : x + y)
custSpentSort = custSpent.map(lambda x:(x[1],x[0])).sortByKey()

custspentamnt = custSpentSort.collect()
for result in custspentamnt:
    print (str(result[1]) + " {:.2f}".format(result[0] ))