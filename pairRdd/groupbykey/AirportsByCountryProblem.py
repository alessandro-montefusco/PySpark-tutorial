from pyspark import SparkContext
import findspark

findspark.init()
'''
    Create a Spark program to read the airport data from in/airports.text,
    output the the list of the names of the airports located in each country.
    
    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
    
    Sample output:
    
    "Canada", ["Bagotville", "Montreal", "Coronation", ...]
    "Norway" : ["Vigra", "Andenes", "Alta", "Bomoen", "Bronnoy",..]
    "Papua New Guinea",  ["Goroka", "Madang", ...]
    ...
'''

if __name__ == "__main__":
    sc = SparkContext(master='local[*]', appName='airports')

    rdd = sc.textFile("../../in/airports.text")
    airports_rdd = rdd.map(lambda line: (line.split(",")[3], line.split(",")[1]))

    groups = airports_rdd.groupByKey().collectAsMap()  # obtain a dictionary

    for key, value in groups.items():
        print("{}: {}".format(key, list(value)))

    sc.stop()

