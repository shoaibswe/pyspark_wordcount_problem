import  module1
# print("indriect call of __name__ is  set to __name__=> name of file",__name__)
if __name__=="__main__":
    print("direct invoke from file1")
    from pyspark import SparkContext
    sc = SparkContext("local[*]", "word count")
    sc.setLogLevel("ERROR")

    inp = sc.textFile("file1.txt")
    words_ = inp.flatMap(lambda x: x.split(" "))
    # count = words_.map(lambda x: (x, 1))
    # count = words_.map(lambda x: (x.lower(), 1)) #lowered all ,else case sensitivity was giving wrong result
    count = words_.map(lambda x:x.lower()) #no need to do rrd single count, coz doing count by value
    # total = count.reduceByKey(lambda x, y: (x + y))
    total= count.countByValue()
    # res = total.collect() #countByValue does not neet collect
    # for i in res:
    #     print(i)
    print(total) #prints dictionary
else:
    print("indirect")