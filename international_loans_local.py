#!/usr/bin/python

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName('python-dataproc-demo') \
    .getOrCreate()

# Defaults to INFO
sc = spark.sparkContext
sc.setLogLevel("WARN")

# Loads CSV file from local directory
dfLoans = spark \
    .read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("data/ibrd-statement-of-loans-latest-available-snapshot.csv")

# Basic stats
print("Rows of data:", dfLoans.count())
print("Inferred Schema:")
dfLoans.printSchema()

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
    "format_number(ABS(total_disbursement), 0) AS total_disbursement, " +
    "format_number(ABS(total_obligation), 0) AS total_obligation, " +
    "format_number(avg_interest_rate, 2) AS avg_interest_rate " +
    "FROM (" +
    "SELECT country, country_code, " +
    "SUM(disbursed) AS total_disbursement, " +
    "SUM(obligation) AS total_obligation, " +
    "AVG(interest_rate) avg_interest_rate " +
    "FROM loans " +
    "GROUP BY country, country_code " +
    "ORDER BY total_disbursement DESC)"
)

dfDisbursement.show(10, 100)

# Saves results to a locally CSV file
dfDisbursement.repartition(1) \
    .write \
    .mode("overwrite") \
    .format("csv") \
    .option("header", "true") \
    .save("data/ibrd-loan-summary")

print("Results successfully written to CSV file")

spark.stop()