from pyspark import SparkContext
import findspark

findspark.init()
'''
    Create a Spark program to read the an article from in/word_count.text,
    output the number of occurrence of each word in descending order.
    
    Sample output:
    
    apple : 200
    shoes : 193
    bag : 176
    ...

'''

if __name__ == "__main__":
    sc = SparkContext(master='local[*]', appName='CountByValue')
    lines = sc.textFile("../../in/word_count.text")

    words = lines.flatMap(lambda line: line.split(" "))  # 1:N relationship
    clearWords = words.map(lambda word: word.replace(",", ""))  # 1:1 relationship
    pair_RDD = words.map(lambda word: (word, 1))
    print(pair_RDD.take(5))

    count_words = pair_RDD.reduceByKey(lambda x, y: x + y)
    print(count_words.take(5))

    sorted_words = count_words.sortBy(lambda x: x[1], ascending=False)

    for item in sorted_words.collect():
        print("{}: {}".format(item[0], item[1]))

    sc.stop()
