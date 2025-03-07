CASE STUDY 1: Movie Rating Analysis

. Generate data with schema: MovialD, UserID, Rating and Timestamp
. Load the dataset into a PySpark DataFrame.
. Convert the Timestamp column to a human-readable date format
. Find the average rating for each movie.
. Identify users who have rated more than 5 movies.
. Determine the top 5 highest-rated movies.
. Visualise the users vs number of movies they rated

CASE STUDY 2: Trip Data Analysis

. Generate data with schema: Trip ID,Start Time, End Time, Distance and Fare
. Load the dataset into a PySpark DataFrame.
. Calculate the trip duration for each trip.
. Find the average fare per mile.
. Identify the top 3 longest trips (based on distance).
. Group trips by hour of the day and calculate the total number of per hour.
. Represent the hours of the day vs number of trips in that hour with suitable chart. 

CASE STUDY 3: Log Analysis

. Generate a log file with the following kind of data: 
  2004-12-18 10:15:32  INFO User logged in
  2024-12-18 10:16:02  ERROR Page not found
  2004-12-18 10:17:20 INFO Data uploaded successfully
  
. log file into a PySpark DataFrame (use spark.read.text)
. Load the Extract the timestamp, log level (INFO, ERROR, etc.), and message
. Count the number of occurrences of each log level.
. Filter and display only the ERROR logs.
. Group logs by hour and count the number of logs in each hour.



