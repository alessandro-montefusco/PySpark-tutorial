from pyspark.sql import SparkSession
import findspark

findspark.init()

'''
   Create a Spark program to read the airport data from in/airports.text,  find all the airports whose latitude are 
   bigger than 40. Then output the airport's name and the airport's latitude to out/airports_by_latitude.text.

   Each row of the input file contains the following columns:
   Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
   ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

   Sample output:
   "St Anthony", 51.391944
   "Tofino", 49.082222
   ...
'''

if __name__ == "__main__":
    spark = SparkSession.builder.appName('Airports_latitude').getOrCreate()
    sc = spark.sparkContext

    rdd = sc.textFile("../../in/airports.text")

    airports_rdd = rdd.map(lambda line: line.split(","))

    filtered_rdd = airports_rdd.filter(lambda airport: float(airport[6]) > 40)

    final_rdd = filtered_rdd.map(lambda airport: [airport[1], airport[6]])

    num_airports = final_rdd.count()
    print("# Airports with latitude greater than 40: ", num_airports)

    df = final_rdd.toDF(['Name', 'Latitude'])
    print(df.show())

    sc.stop()
