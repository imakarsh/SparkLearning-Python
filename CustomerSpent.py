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

custspentamnt = custSpent.collect()
for result in custspentamnt:
    print (str(result[0]) + " {:.2f}".format(result[1] ))