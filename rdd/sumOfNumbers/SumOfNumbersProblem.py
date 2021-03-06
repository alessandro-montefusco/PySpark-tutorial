from pyspark import SparkContext, SparkConf
import findspark
findspark.init()

'''
    Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
    print the sum of those numbers to console.
    Each row of the input file contains 10 prime numbers separated by spaces.
'''

if __name__ == "__main__":
    conf = SparkConf().setAppName("primeNumbers").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    lines = sc.textFile("../../in/prime_nums.text")
    numbers = lines.flatMap(lambda line: line.split("\t"))

    validNumbers = numbers.filter(lambda number: number)

    intNumbers = validNumbers.map(lambda number: int(number))

    print("Sum is: {}".format(intNumbers.reduce(lambda x, y: x + y)))

    sc.stop()