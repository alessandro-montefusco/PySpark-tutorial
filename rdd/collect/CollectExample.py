from pyspark import SparkContext, SparkConf
import findspark

findspark.init()

if __name__ == "__main__":
    conf = SparkConf().setAppName("collect").setMaster("local[*]")
    sc = SparkContext(conf = conf)
    
    inputWords = ["spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop"]
    
    wordRdd = sc.parallelize(inputWords)
    
    words = wordRdd.collect()
    
    for word in words:
        print(word)

    sc.stop()
