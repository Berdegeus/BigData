from pyspark.sql import SparkSession

# Step 1: Initialize SparkSession
spark = SparkSession.builder \
    .appName("USA Airports") \
    .getOrCreate()

# Step 2: Load the Airports.csv file as an RDD
airports_rdd = spark.sparkContext.textFile("Airports.csv")

# Step 3: Remove the header
header = airports_rdd.first()
airports_rdd = airports_rdd.filter(lambda line: line != header)

# Step 4: Split each line by commas to access individual columns
airports_rdd = airports_rdd.map(lambda line: line.split(","))

# Step 5: Filter for airports in the USA (assuming 'Country' is the 5th column)
usa_airports_rdd = airports_rdd.filter(lambda fields: fields[4] == "USA")
br_airports_rdd = airports_rdd.filter(lambda fields: fields[4] == "BRAZIL")

# Step 6: Display all USA airports
for airport in usa_airports_rdd.collect():
    print(airport)

# Step 7: Count the number of USA airports
usa_airports_count = usa_airports_rdd.count()
print(f"Number of airports in the USA: {usa_airports_count}")
br_airports_count = br_airports_rdd.count()
print(f"Number of airports in Brazil: {br_airports_count}")

# Step 8: Save the list of USA airports as a single text file
usa_airports_names = usa_airports_rdd.map(lambda fields: fields[2])
usa_airports_names.saveAsTextFile("USA_Airports_Names")
br_airports_names = br_airports_rdd.map(lambda fields: fields[2])
br_airports_names = br_airports_names.filter(lambda fields: fields[2] if fields != 'N/A' and fields != 'UNKNOWN' else None)
br_airports_names.saveAsTextFile("BR_Airports_Names")

# Step 9: Stop the Spark session
spark.stop()
