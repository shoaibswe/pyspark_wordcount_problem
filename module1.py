import sys

# from datetime import date
# print(date.today())
# print("running directly Global var set to __name__ => main ", __name__)

if __name__ == "__main__":
    print("direct invoke")
    from pyspark import SparkContext
    sc = SparkContext("local[*]", "word count")
    # sc.setLogLevel("INFO")
    sc.setLogLevel("ERROR")

    inp = sc.textFile("file1.txt")
    words_ = inp.flatMap(lambda x: x.split(" "))
    # count = words_.map(lambda x: (x, 1))
    count = words_.map(lambda x: (x.lower(), 1)) #lowered all ,else case sensitivity was giving wrong result
    total = count.reduceByKey(lambda x, y: (x + y))
    res = total.collect()
    for i in res:
        print(i)
else:
    print("indirect file1")

# sys.stdin.readline() #Waits for user input to run