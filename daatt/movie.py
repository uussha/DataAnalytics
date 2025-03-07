from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, avg, count
import pandas as pd
import matplotlib.pyplot as plt

# Initialize Spark Session
spark = SparkSession.builder.appName("MovieRatingAnalysis").getOrCreate()

# Sample Data: (MovieID, UserID, Rating, Timestamp)
data = [
    (1, 101, 4.5, 1672531200),
    (1, 102, 3.0, 1672617600),
    (2, 101, 5.0, 1672704000),
    (2, 103, 4.0, 1672790400),
    (3, 101, 2.5, 1672876800),
    (3, 104, 3.5, 1672963200),
    (4, 102, 4.0, 1673049600),
    (5, 101, 3.0, 1673136000),
    (5, 103, 4.5, 1673222400),
    (6, 101, 4.0, 1673308800),
    (6, 102, 3.5, 1673395200),
    (7, 101, 5.0, 1673481600),
    (8, 104, 2.5, 1673568000),
    (9, 101, 3.0, 1673654400),
    (10, 101, 4.5, 1673740800),
]

# Define Schema
columns = ["MovieID", "UserID", "Rating", "Timestamp"]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Convert Timestamp to Human-Readable Date
df = df.withColumn("Date", from_unixtime(col("Timestamp"), "yyyy-MM-dd"))

# Display the DataFrame
print("Original Data with Readable Dates:")
df.show()

# Calculate Average Rating for Each Movie
avg_ratings = df.groupBy("MovieID").agg(avg("Rating").alias("AvgRating"))
print("Average Rating for Each Movie:")
avg_ratings.show()

# Identify Users Who Have Rated More Than 5 Movies
user_counts = df.groupBy("UserID").agg(count("MovieID").alias("MovieCount"))
active_users = user_counts.filter(col("MovieCount") > 5)
print("Users Who Rated More Than 5 Movies:")
active_users.show()

# Determine the Top 5 Highest-Rated Movies
top_movies = avg_ratings.orderBy(col("AvgRating").desc()).limit(5)
print("Top 5 Highest-Rated Movies:")
top_movies.show()

# Convert Data to Pandas for Visualization
user_counts_pd = user_counts.toPandas()

# Plot Users vs Number of Movies Rated
plt.figure(figsize=(10, 5))
plt.bar(user_counts_pd["UserID"], user_counts_pd["MovieCount"], color='skyblue')
plt.xlabel("User ID")
plt.ylabel("Number of Movies Rated")
plt.title("Users vs Number of Movies Rated")
plt.xticks(rotation=45)
plt.show()

# Stop Spark Session
spark.stop()
