import os

from pyspark import SparkContext
sc = SparkContext("local[*]", "word count")
# sc.setLogLevel("ERROR")

try:
    # os.environ['HADOOP_HOME'] = "C:\\winutils" #downloaded from https://github.com/steveloughran/winutils/tree/master/hadoop-2.7.1/bin
    # print(os.environ['HADOOP_HOME'])

    inp = sc.textFile("file1.txt")
    words_ = inp.flatMap(lambda x: x.split(" "))
    count = words_.map(lambda x: (x.lower(), 1)) #lowered all ,else case sensitivity was giving wrong result

   #--one way to sort (USING sortByKey)
    # total = count.reduceByKey(lambda x, y: (x + y)).map(lambda x: (x[1], x[0])) #getting val as key to sort
    # res = total.sortByKey(False).map(lambda x: (x[1], x[0])).collect() #again mapped to set val as val and key as key

    #--Another easy way to sort  (USING sortBy)
    total = count.reduceByKey(lambda x, y: (x + y))
    res = total.sortBy(lambda x: x[1], False).collect()  #sortBy sorted on 2nd index/or value

    for i in res:
        print(i)
except Exception as e:
    print("Error : ", e)
#NOW SHOWS IN DESC ORDER COUNT