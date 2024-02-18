# Retail-ETL-Pipeline Project

## Project Objective:
In this Data Pipeline project, CloudWatch is employed to schedule Lambda functions responsible for scanning an S3 bucket. Upon completion of the scanning process and determination that the required data is fully available, a signal is sent to Apache Airflow. Airflow is then triggered to coordinate the execution of tasks, including parsing requests, running Apache Spark on AWS EMR, and monitoring the process through a watch step. The processed and cleaned data is subsequently stored in the Parquet format on Amazon S3. AWS Glue and Athena are utilized for direct data crawling and querying, so that the BI team can connect to my Athena for Visualization.

![AWS ETL Pipeline](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/assets/131537129/5acb93f1-e008-4eae-b339-1489d3e86af5)

## Detailed Steps

### a. S3 Bucket
In this step, I first dump transaction data from a transaction database. In this project, I use Snowflake to pretend the transaction database and dump the data into the s3 bucket on schedule.

1). Load data into Snowflake.<br> 
In order to mimic Snowflake as a transaction database, I need to load the transaction data first. Used to load data into Snowflake. This is a set of data, including dimension tables, store, product, calendar, and fact tables sales and inventory. [Check Script]([https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Snowflake/Dump%20Data.sql](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Snow%20Flake/Data%20Ingestion.sql))

2). Create a S3 bucket and S3 Stage in Snowflake.<br>
After loading the data into Snowflake, I have to prepare the S3 bucket in AWS for raw data dumping. But I cannot dump data directly into S3 bucket until I create a S3 integration Stage in Snowflake.
[Check Script](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Snow%20Flake/Storage%20Integration.sql)

3). Load data to the S3 datawarehouse.<br>
After creating integration, I can load the data into the s3 datawarehouse.
[Check Script](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Snow%20Flake/Data%20Load.sql)

### b.Cloudwatch + Lambda
Since we have multiple files dumped to S3 separately, it is not very convenient to use S3 trigger for Lambda because we don’t know when to trigger the Lambda. Instead of using S3 event triggers for lambda, we use Cloudwatch to schedule the Lambda function.

1).Cloudwatch.<br>
I will create a Cloudwatch schedule, use Cron expression to plan when to trigger lambda. I set up to run trigger the Lambda everyday 4:00am.

2). AWS Lambda Function.<br>
Lambda function will do several tasks: Scan S3 bucket to check if all required files are ready, when all are ready, send a signal to Airflow to start EMR. [Check Script]( https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Lambda%20Function.py)

3). AWS SES Service.<br>
If the fiels in the s3 bucket is not ready. Email will be sent to me by AWS SES as a remainder.[Check Script](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/AWS%20SES.py)

### c. Airflow
After Lambda send the signal to Airflow, it will do three things to orchestrate the process: 1.parse_request: retrive the data from s3 bucket. 2.step_adder: Run the PySpark on the EMR to do the data transformation. 3.step_checker: Watch the steps status.

1). Since my Airflow is installed on EC2 instance, I need a new IAM Role for the EC2 instance to communicate with EMR cluster.
2). Run the Airflow for Orchestration.[Check Script](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Airflow%20Dag.py)

### d. EMR
EMR is triggered by Airflow in the preceding step. EMR will use the files in the S3 bucket where raw data is dumped. Pyspark running in EMR will do the following tasks:<br>
Task 1: Read data from S3.<br>
Task 2: Do data transformation process to generate a dataframe to meet the following business requirement.<br>

The table will be grouped by each week, each store, each product to calculate the following metrics:<br>
total sales quantity of a product : Sum(sales_qty)<br>
total sales amount of a product : Sum(sales_amt)<br>
average sales Price: Sum(sales_amt)/Sum(sales_qty)<br>
stock level by the end of the week : inventory_on_hand_qty by the end of the week (only the stock level at the end day of the week)<br>
inventory on order level by the end of the week: ordered_inventory_qty by the end of the week (only the ordered stock quantity at the end day of the week)<br>
total cost of the week: Sum(sales_cost)<br>
the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)<br>
total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)<br>
potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)<br>
no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)<br>
low Stock Instances: Calculate how many times of Low_Stock_Flg in a week<br>
no Stock Instances: Calculate then how many times of out_of_Stock_Flg in a week<br>
how many weeks the on hand stock can supply: (inventory_on_hand_qty at the end of the week) / sum(sales_qty)<br>
Task 3: After above transformation, save the final dataframe as a parquet file to a new S3 output folder and will be used for later data analysis with Athena.
[Check Script](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/blob/main/Data%20Transformation.py)

### e. AWS Athena and Glue
I use AWS Glue feature Crawler to crawl the metadata and schemas of saved parquet output file and then use AWS Athena to do the query for analysis. So that the BI team can do the Visulization based on my output.

1). AWS Glue<br>
I have to create a new Bucket in the S3 to store the metadata table. <br>
I create a database in the AWS Glue<br>
![Screenshot 2024-02-18 133223](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/assets/131537129/44ca7dd4-d45d-41da-9ee7-9a338d14b864)
Run the Crawler to crawl my parquet file to get schemas and metadata and store into my S3 bucket.<br>
![Screenshot 2024-02-18 133520](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/assets/131537129/88883f8c-d8b7-41a5-8326-f830bdde14cd)

2). Athena<br>
I use Athena to do the query for the output fiel from previous step.
![Screenshot 2024-02-18 135040](https://github.com/LeoMeng5223655/Retail-ETL-Pipeline/assets/131537129/6575a691-200f-4c91-91c1-e8a42d3c2f69)

Then BI Team can connect to Athena for visualization!!!













