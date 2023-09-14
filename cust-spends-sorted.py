from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster('local').setAppName("Custmer_total")
sc=SparkContext(conf=conf)

def cust_details(line):
    fields=line.split(',')
    custId=int(fields[0])
    custSpends=float(fields[2])
    
    return (custId,custSpends)
    

input=sc.textFile("file:///C:/Users/dell/Desktop/SparkCourse/customer-orders.csv")
rdd=input.map(cust_details)

totalSpend=rdd.reduceByKey(lambda x,y: x+y)
totalSpendSorted=totalSpend.map(lambda x: (x[1],x[0])).sortByKey()
results=totalSpendSorted.collect()

for result in results:
    print(result)
