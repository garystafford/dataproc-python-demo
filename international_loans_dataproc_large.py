#!/usr/bin/python

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName('dataproc-python-demo') \
    .getOrCreate()

# Defaults to INFO
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Loads CSV file from Google Storage Bucket
dfLoans = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("gs://dataproc-demo-bucket/ibrd-statement-of-loans-historical-data.csv")

# Creates temporary view using DataFrame
dfLoans.withColumnRenamed("Country", "country") \
    .withColumnRenamed("Country Code", "country_code") \
    .withColumnRenamed("Disbursed Amount", "disbursed") \
    .withColumnRenamed("Borrower's Obligation", "obligation") \
    .withColumnRenamed("Interest Rate", "interest_rate") \
    .createOrReplaceTempView("loans")

# Performs basic analysis of dataset
dfDisbursement = spark.sql(
    "SELECT country, country_code, " +
    "format_number(total_disbursement, 0) AS total_disbursement, " +
    "format_number(ABS(total_obligation), 0) AS total_obligation, " +
    "format_number(avg_interest_rate, 2) AS avg_interest_rate " +
    "FROM (" +
    "SELECT country, country_code, " +
    "SUM(disbursed) AS total_disbursement, " +
    "SUM(obligation) AS total_obligation, " +
    "AVG(interest_rate) avg_interest_rate " +
    "FROM loans " +
    "GROUP BY country, country_code " +
    "ORDER BY total_disbursement DESC " +
    "LIMIT 25)"
)

# Saves results to single CSV file in Google Storage Bucket
dfDisbursement.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save("gs://dataproc-demo-bucket/ibrd-loan-summary-large-python")

print("Results successfully written to CSV file")
