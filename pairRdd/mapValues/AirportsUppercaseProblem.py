from pyspark import SparkContext
import findspark

findspark.init()
'''
    Create a Spark program to read the airport data from in/airports.text, generate a pair RDD with airport name
    being the key and country name being the value. Then convert the country name to uppercase and
    output the pair RDD to out/airports_uppercase.text

    Each row of the input file contains the following columns:

    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

    Sample output:

    ("Kamloops", "CANADA")
    ("Wewak Intl", "PAPUA NEW GUINEA")
    ...

'''

if __name__ == "__main__":
    sc = SparkContext(master='local[*]', appName='airports')

    rdd = sc.textFile("../../in/airports.text")
    airports_rdd = rdd.map(lambda line: line.split(","))
    airports = airports_rdd.map(lambda airport: (airport[1], airport[3]))
    print(airports.take(5))

    uppercase = airports.mapValues(lambda airport: airport.upper())

    for item in uppercase.collect():
        print("{}: {}".format(item[0], item[1]))

    sc.stop()
