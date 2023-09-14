from pyspark import SparkConf, SparkContext

conf=SparkConf().setMaster('local').setAppName("Custmer_total")
sc=SparkContext(conf=conf)

def cust_details(line):
    fields=line.split(',')
    custId=int(fields[0])
    custSpends=float(fields[1])
    
    return (custId,custSpends)
    

input=sc.textFile("file:///C:/Users/dell/Desktop/SparkCourse/customer-orders.csv")
rdd=input.map(cust_details)

totalSpend=rdd.reduceByKey(lambda x,y: x+y).sortByKey()
results=totalSpend.collect()

for result in results:
    print(result)
