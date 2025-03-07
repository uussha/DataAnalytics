from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, unix_timestamp, avg, count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("TripDataAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("Spark session created")

# Define schema
schema = StructType([
    StructField("Trip_ID", IntegerType(), True),
    StructField("Start_Time", StringType(), True),
    StructField("End_Time", StringType(), True),
    StructField("Distance", FloatType(), True),
    StructField("Fare", FloatType(), True)
])
print("Schema defined")

# Sample Data
data = [
    (1, "2025-03-06 08:15:00", "2025-03-06 08:45:00", 12.5, 250.0),
    (2, "2025-03-06 09:10:00", "2025-03-06 09:50:00", 18.0, 320.0),
    (3, "2025-03-06 10:05:00", "2025-03-06 10:40:00", 25.2, 450.0),
    (4, "2025-03-06 11:20:00", "2025-03-06 11:50:00", 8.0, 150.0),
    (5, "2025-03-06 12:30:00", "2025-03-06 13:10:00", 15.6, 280.0)
]
print("Sample data created")

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)
print("DataFrame created")
df.show()

# Compute trip duration
df = df.withColumn("Trip_Duration", (unix_timestamp("End_Time") - unix_timestamp("Start_Time")) / 60)
print("Trip duration calculated")
df.select("Trip_ID", "Trip_Duration").show()

# Compute fare per mile
df = df.withColumn("Fare_per_Mile", col("Fare") / col("Distance"))
avg_fare_per_mile = df.agg(avg("Fare_per_Mile")).collect()[0][0]
print(f"Average Fare per Mile: {avg_fare_per_mile:.2f}")

# Find top 3 longest trips
longest_trips = df.orderBy(col("Distance").desc()).limit(3)
print("Top 3 longest trips identified")
longest_trips.show()

# Group trips by hour
df = df.withColumn("Hour", hour("Start_Time"))
hourly_trips = df.groupBy("Hour").agg(count("Trip_ID").alias("Total_Trips"))
print("Trips grouped by hour")
hourly_trips.show()

# Write output to file
hourly_trips_pd = hourly_trips.toPandas()
with open("tripout.txt", "w") as f:
    f.write(f"Average Fare per Mile: {avg_fare_per_mile:.2f}\n")
    f.write("\nTrip Durations:\n" + df.select("Trip_ID", "Trip_Duration").toPandas().to_csv(index=False))
    f.write("\nTop 3 Longest Trips:\n" + longest_trips.toPandas().to_csv(index=False))
    f.write("\nHourly Trip Counts:\n" + hourly_trips_pd.to_csv(index=False))
print("Output written to file")

# Visualization
hourly_trips_pd.plot(x='Hour', y='Total_Trips', kind='bar', legend=False, color='blue')
plt.xlabel("Hour of the Day")
plt.ylabel("Number of Trips")
plt.title("Trips Per Hour")
plt.xticks(rotation=0)
plt.savefig("trips_per_hour.png")
print("Plot saved as 'trips_per_hour.png'")
plt.show()

print("Script execution completed successfully!")
