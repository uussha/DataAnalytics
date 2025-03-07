from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, hour

# Initialize Spark Session
spark = SparkSession.builder.appName("LogAnalysis").getOrCreate()

# Load log file into DataFrame
log_df = spark.read.text("log_data.txt")

# Define regex pattern to extract log details
pattern = r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) (\w+) (.+)"

# Extract timestamp, log level, and message
logs_parsed_df = log_df.select(
    regexp_extract(col("value"), pattern, 1).alias("timestamp"),
    regexp_extract(col("value"), pattern, 2).alias("log_level"),
    regexp_extract(col("value"), pattern, 3).alias("message")
)

# Count occurrences of each log level
log_level_counts = logs_parsed_df.groupBy("log_level").agg(count("*").alias("count"))
print("Log Level Counts:")
log_level_counts.show()

# Filter ERROR logs
error_logs = logs_parsed_df.filter(col("log_level") == "ERROR")
print("ERROR Logs:")
error_logs.show()

# Group logs by hour and count them
logs_by_hour = logs_parsed_df.withColumn("hour", hour(col("timestamp"))) \
    .groupBy("hour").agg(count("*").alias("log_count"))

print("Logs Count Per Hour:")
logs_by_hour.show()

# Stop Spark Session
spark.stop()
