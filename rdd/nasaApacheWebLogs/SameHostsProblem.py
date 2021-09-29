from pyspark import SparkContext, SparkConf
import findspark

findspark.init()

'''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
    Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

    Example output:
    vagrant.vf.mmc.com
    www-a1.proxy.aol.com
    .....    

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
'''

if __name__ == "__main__":
    sc = SparkContext(appName="Intersection", master='local[*]')

    rdd_july = sc.textFile("../../in/nasa_19950701.tsv")
    rdd_august = sc.textFile("../../in/nasa_19950801.tsv")

    host_july = rdd_july.map(lambda line: line.split("\t")[0]).filter(lambda line: "host" not in line)
    host_august = rdd_august.map(lambda line: line.split("\t")[0]).filter(lambda line: "host" not in line)

    intersection = host_july.intersection(host_august)
    hosts = intersection.collect()

    print("# Hosts visited in the same day on both months: ", intersection.count())
    for h in hosts:
        print(h)

    sc.stop()
