from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
        
        
        
        
        
# from pyspark import SparkContext,SparkConf 

# conf=SparkConf().setMaster('local').setAppName("WordCount")
# sc=SparkContext(conf=conf)

# input=sc.textFile("file:///C:/Users/dell/Desktop/SparkCourse/Book.txt")
# words=input.flatMap(lambda x: x.split())
# wordsCount=words.countByValue()

# for word, count in wordsCount.items():
#     cleanWord= word.encode('ascii','ignore')
#     if (cleanWord):
#         print(cleanWord, count)