import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input=sc.textFile("file:///C:/Users/dell/Desktop/SparkCourse/Book.txt")
words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
        

# import re
# from pyspark import SparkConf, SparkContext

# conf=SparkConf().setMaster('local').setAppName('WordKeySorted')
# sc=SparkContext(conf=conf)

# def normalizeWords(text):
#     return re.compile(r'W+', re.UNICODE).split(text.lower())

# input=sc.textFile("file:///C:/Users/dell/Desktop/SparkCourse/Book.txt")
# words=input.map(normalizeWords)

# wordCounts=words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
# wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
# results=wordCountsSorted.collect()

# for result in results:
#     count=str(result[0])
#     word=result[1].encode('ascii','ignore')
#     if (word):
#         print(word.decode() +":" +count)