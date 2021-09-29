from pyspark import SparkContext, SparkConf
import findspark

findspark.init()

'''
    "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
    "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
    Create a Spark program to generate a new RDD which contains the log lines from both July 1st and August 1st,
    take a 0.1 sample of those log lines and save it to "out/sample_nasa_logs.tsv" file.

    Keep in mind, that the original log files contains the following header lines.
    host    logname    time    method    url    response    bytes

    Make sure the head lines are removed in the resulting RDD.
'''

if __name__ == "__main__":
    sc = SparkContext(appName="Union", master='local[*]')

    rdd_july = sc.textFile("/content/drive/MyDrive/in/nasa_19950701.tsv")
    rdd_august = sc.textFile("/content/drive/MyDrive/in/nasa_19950801.tsv")

    host_july = rdd_july.map(lambda line: line.split("\t")[0]).filter(lambda line: "host" not in line)
    host_august = rdd_august.map(lambda line: line.split("\t")[0]).filter(lambda line: "host" not in line)

    all_hosts = host_july.union(host_august)
    print("Total # hosts visited on both months: ", all_hosts.count())

    sampling = all_hosts.sample(withReplacement=False, fraction=0.1, seed=12345)
    all_samples = sampling.coalesce(1)
    all_samples.saveAsTextFile("/content/drive/MyDrive/out/sample_nasa_logs.csv")

    samples = sampling.collect()
    print("# extracted samples: ", len(samples))

    sc.stop()
