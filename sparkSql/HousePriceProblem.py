from pyspark.sql import SparkSession
'''    
    Create a Spark program to read the house data from in/RealEstate.csv,
    group by location, aggregate the average price per SQ Ft and sort by average price per SQ Ft.

    The dataset contains the following fields:
    MLS; Location; Price; Number of Bedrooms; Number of Bathrooms; Size; Price per SQ Ft; 
    Status (Short Sale,	Foreclosure and Regular)

    Each field is comma separated.

    Sample output:

    +----------------+-----------------+
    |        Location| avg(Price SQ Ft)|
    +----------------+-----------------+
    |          Oceano|             95.0|
    |         Bradley|            206.0|
    | San Luis Obispo|            359.0|
    |      Santa Ynez|            491.4|
    |         Cayucos|            887.0|
    |................|.................|
    |................|.................|
    |................|.................|
'''
if __name__ == "__main__":
    session = SparkSession.builder.appName("Join_operations").master("local[*]").getOrCreate()
    df = session.read.csv(path="/content/drive/MyDrive/in/RealEstate.csv", header=True, inferSchema=True)

    df = df.groupBy('Location').avg('Price SQ Ft')
    df = df.withColumnRenamed("avg(Price SQ Ft)", 'Average Price')
    df = df.sort('Average Price')
    print(df.show())

    session.stop()
