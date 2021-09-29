from pyspark import SparkContext, SparkConf
import findspark

findspark.init()

if __name__ == "__main__":
    conf = SparkConf().setAppName("reduce").setMaster("local[*]")
    sc = SparkContext(conf=conf)
   
    inputIntegers = [i for i in range(8)]
    integerRdd = sc.parallelize(inputIntegers)
   
    product = integerRdd.reduce(lambda x, y: x * y)
    print("product is :{}".format(product))

    sc.stop()
