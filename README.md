# Retail-ETL-Pipeline Project

## Project Objective:
Retail ETL Pipeline project 
In this Data Pipeline project, CloudWatch is employed to schedule Lambda functions responsible for scanning an S3 bucket. Upon completion of the scanning process and determination that the required data is fully available, a signal is sent to Apache Airflow. Airflow is then triggered to coordinate the execution of tasks, including parsing requests, running Apache Spark on AWS EMR, and monitoring the process through a watch step. The processed and cleaned data is subsequently stored in the Parquet format on Amazon S3. AWS Glue and Athena are utilized for direct data crawling, facilitating BI downstream usage through efficient querying capabilities.

![AWS ETL Pipeline](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/assets/131537129/5acb93f1-e008-4eae-b339-1489d3e86af5)

## Detailed Steps
In this step, we first dump transaction data from a transaction database. In this midterm project, we use Snowflake to pretend the transaction database. (Please note that Snowflake is a data warehouse that cannot be used as a transaction database. Here we just pretend Snowflake is a transaction database.)

Why use Snowflake? The reason we use Snowflake to pretend to be a real transaction database, like mysql, is that we only want to simulate the automatic process where raw data is dumped into the S3 bucket on schedule.

The start point of the portfolio project is the point where raw data is dumped into the S3 bucket, so it is not important to choose which database to simulate the scenario. For this reason, we use snowflake, because snowflake is easier to manage and schedule tasks for you. But, be aware that we dump raw data from a transaction database NOT a data warehouse.

In order to schedule dumping data to S3 bucket from Snowflake, we need to follow the below steps:

1). Create a Snowflake Account.
If your Snowflake account has expired, please create a new account.

2). Load data into Snowflake.
In order to mimic Snowflake as a transaction database, you need to load the transaction data first. Use below (script) to load data into Snowflake. This is a set of data, including dimension tables, store, product, calendar, and fact tables sales and inventory. This is very similar to what we used in data warehousing. The only difference is that the date is up to date. So you will have a chance to use “today’s” data. 
