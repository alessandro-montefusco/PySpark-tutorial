import pandas as pd
from pyspark import SparkContext, SparkConf
import findspark

findspark.init()
'''
  Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
  and output the airport's name and the city's name to out/airports_in_usa.text.

  Each row of the input file contains the following columns:
  Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
  ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format

  Sample output:
  "Putnam County Airport", "Greencastle"
  "Dowagiac Municipal Airport", "Dowagiac"
  ...
'''

if __name__ == "__main__":
    sc = SparkContext(appName="Airports", master="local[*]")

    airports_rdd = sc.textFile("../../in/airports.text")
    # print(airports_rdd.take(2))
    usa_airports_rdd = airports_rdd.filter(lambda airport: "United States" in airport).map(lambda line: line.split(","))
    # print(usa_airports_rdd.take(2))
    usa_airports_NameAndCity_rdd = usa_airports_rdd.map(lambda line: [line[1], line[2]])
    # print(usa_airports_NameAndCity_rdd.take(2))
    #usa_airports_NameAndCity_rdd.saveAsTextFile("/content/drive/MyDrive/out/airports_results")

    num_airports_usa = usa_airports_NameAndCity_rdd.count()
    usa_airports = usa_airports_NameAndCity_rdd.collect()
    df_usa = pd.DataFrame(usa_airports, columns=['Airport Name', 'Airport City'])

    print("Airports in USA: ", num_airports_usa)
    print(df_usa.to_string(index=False))

    sc.stop()
