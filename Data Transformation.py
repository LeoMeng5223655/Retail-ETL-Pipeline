from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, BooleanType
from pyspark.sql import functions as F
import sys

if __name__ == "__main__":
    # define the date and file inputs
    date_str = sys.argv[1]
    file_inputs = sys.argv[2].split(',')

    # start the spark session local[*] is for testing
    spark = SparkSession \
        .builder \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "your_access_key") \
        .config("spark.hadoop.fs.s3a.secret.key", "your_secret_key") \
        .master("Yarn")\
        .appName("aws_project") \
        .getOrCreate()

    # Define the file paths and DataFrame names in a dictionary for testing
    file_mapping = {
        "calendar_df": file_inputs[0],
        "inventory_df": file_inputs[1],
        "product_df": file_inputs[2], 
        "sales_df": file_inputs[3],
        "store_df": file_inputs[4],
    }

    # read files from the s3
    for key, value in file_mapping.items():
        file_mapping[key] = spark.read.csv(value, header=True, inferSchema=True)

    # Assign and Create each DataFrame to its corresponding variable
    inventory_df = file_mapping["inventory_df"]
    sales_df = file_mapping["sales_df"]
    calendar_df = file_mapping["calendar_df"]
    store_df = file_mapping["store_df"]
    product_df = file_mapping["product_df"]
    
    # Change data type of CAL_DT, NEXT_DELIVERY_DT, TRANS_DT, CAL_DT to DateType
    inventory_df = inventory_df.withColumn("CAL_DT", to_date(inventory_df["CAL_DT"], "yyyy-MM-dd"))
    inventory_df = inventory_df.withColumn("NEXT_DELIVERY_DT", to_date(inventory_df["NEXT_DELIVERY_DT"], "yyyy-MM-dd"))
    sales_df = sales_df.withColumn("TRANS_DT", to_date(sales_df["TRANS_DT"], "yyyy-MM-dd"))
    calendar_df = calendar_df.withColumn("CAL_DT", to_date(calendar_df["CAL_DT"], "yyyy-MM-dd"))

    # Join the dataframes together for each week, each store, each product using TRANS_DT == CAL_DT, PROD_KEY, STORE_KEY
    merged_df = sales_df.join(inventory_df, ["PROD_KEY","STORE_KEY"]) \
                        .withColumnRenamed("CAL_DT","INVENTORY_CAL_DT") \
                        .join(calendar_df, sales_df["TRANS_DT"] == calendar_df["CAL_DT"]) \
                        .join(store_df, "STORE_KEY") \
                        .join(product_df, "PROD_KEY") 

     # merged_df.show()

    # merged_df.createOrReplaceTempView("merged_df")

    # Create Dataframe for total sales amount, total sales quantity, average sales price, and total cost of the week
    # Group by each week, each store and each product
    df_sum_sales_amt_qty_cost = merged_df.groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY") \
                                        .agg(round(sum("sales_amt"),2).alias("total_sales_amt"), \
                                        round(sum("sales_qty"),2).alias("total_sales_qty"), \
                                        round(sum("sales_amt")/sum("sales_qty"),2).alias("avg_sales_amt"), \
                                        round(sum("sales_cost"),2).alias("total_sales_cost")) \
                                        .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_sum_sales_amt_qty_cost.show()

    # stock level by the end of the week : inventory_on_hand_qty by the end of the week (only the stock level at the end day of the week)
    df_stock_level = merged_df.withColumn("row_num", F.row_number().over(Window.partitionBy("YR_WK_NUM","STORE_KEY","PROD_KEY").orderBy(F.desc("CAL_DT")))) \
                              .where(F.col("row_num") == 1) \
                              .select("YR_WK_NUM","STORE_KEY","PROD_KEY","INVENTORY_ON_HAND_QTY") \
                              .withColumnRenamed("INVENTORY_ON_HAND_QTY","INVENTORY_ON_HAND_QTY_END_WK")
    
      # df_stock_level.show()

    # inventory on order level by the end of the week: ordered_inventory_qty by the end of the week (only the ordered stock quantity at the end day of the week)
    df_stock_on_order = merged_df.withColumn("row_num", F.row_number().over(Window.partitionBy("YR_WK_NUM","STORE_KEY","PROD_KEY").orderBy(F.desc("CAL_DT")))) \
                              .where(F.col("row_num") == 1) \
                              .select("YR_WK_NUM","STORE_KEY","PROD_KEY","INVENTORY_ON_ORDER_QTY") \
                              .withColumnRenamed("INVENTORY_ON_ORDER_QTY","INVENTORY_ON_ORDER_QTY_END_WK")

    # df_stock_on_order.show()
    
    # Total Cost of the week
    df_sum_cost = merged_df.groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY") \
                                        .agg(round(sum("sales_cost"),2).alias("total_cost_week")) \
                                        .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_sum_cost.show()

    # the percentage of Store In-Stock: (how many times of out_of_stock in a week) / days of a week (7 days)
    df_out_of_stock = merged_df.groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY") \
                               .agg(round(sum("out_of_stock_flg")/7, 2).alias("out_of_stock_count")) \
                               .select("YR_WK_NUM","STORE_KEY","PROD_KEY","out_of_stock_count") \
                               .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_out_of_stock.show()

    # ow Stock_flg = if today's inventory_on_hand_qty<sales_qty , then low_stock_flg=1, otherwise =0) 
    df_low_stock = merged_df.withColumn("low_stock_flg", when(col("INVENTORY_ON_HAND_QTY") < col("SALES_QTY"),1).otherwise(0)) \
                                .select("YR_WK_NUM","STORE_KEY","PROD_KEY","low_stock_flg")
    # df_low_stock.show()

    # create temporary view for df_low_stock 
    df_low_stock.createOrReplaceTempView("df_low_stock")

    # total Low Stock Impact: sum (out_of+stock_flg + Low_Stock_flg)
    # subquery m
    window_spec_m = Window.partitionBy("STORE_KEY","PROD_KEY","CAL_DT").orderBy("CAL_DT") 
    m = merged_df.withColumn("row_num", row_number().over(window_spec_m)) 
    m = m.filter(col("out_of_stock_flg") == 1) 
    m = m.groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY").agg(count("out_of_stock_flg").alias("out_of_stock_count"))
    
    # m.show()
  
    # # subquery t2
    t2 = df_low_stock.groupBy("STORE_KEY","PROD_KEY").agg(count("low_stock_flg").alias("low_stock_count"))
    # t2.show()
    
    # low_stock_impact
    df_low_stock_impact = m.join(t2, ["STORE_KEY","PROD_KEY"])
    df_low_stock_impact = df_low_stock_impact.select(
        df_low_stock_impact["YR_WK_NUM"],
        df_low_stock_impact["STORE_KEY"],
        df_low_stock_impact["PROD_KEY"],
        (df_low_stock_impact["out_of_stock_count"] + df_low_stock_impact["low_stock_count"]).alias("total_low_stock_impact")).orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
     
    # df_low_stock_impact.printSchema()
    # df_low_stock_impact.show() 

    # potential Low Stock Impact: if Low_Stock_Flg =TRUE then SUM(sales_amt - stock_on_hand_amt)
    df_potential_low_stock_impact = merged_df.join(df_low_stock, ["YR_WK_NUM","STORE_KEY","PROD_KEY"], "inner") \
        .filter(col("low_stock_flg") == 1) \
        .groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY") \
        .agg(round(sum(col("SALES_QTY")) - sum(col("INVENTORY_ON_HAND_QTY")),2).alias("potential_low_stock_impact")) \
        .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_potential_low_stock_impact.show()


    # no Stock Impact: if out_of_stock_flg=true, then sum(sales_amt)
    df_no_stock_instances = merged_df.join(df_low_stock, ["YR_WK_NUM","STORE_KEY","PROD_KEY"], "inner") \
        .filter(col("out_of_stock_flg") == 1) \
        .groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY") \
        .agg(count("out_of_stock_flg").alias("no_stock_instances")) \
        .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_no_stock_instances.show()


    # how many weeks the on hand stock can supply: (inventory_on_hand_qty at the end of the week) / sum(sales_qty)
    df_weeks_supply = merged_df.groupBy("YR_WK_NUM","STORE_KEY","PROD_KEY","INVENTORY_ON_HAND_QTY") \
        .agg(round((col("INVENTORY_ON_HAND_QTY") / sum(col("sales_qty"))),2).alias("weeks_supply")) \
        .orderBy("YR_WK_NUM","STORE_KEY","PROD_KEY")
    # df_weeks_supply.show()

    # Final Dataframe
    df_final = df_sum_sales_amt_qty_cost.alias("df_sum") \
        .join(df_stock_level.alias("df_stock_level"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_stock_on_order.alias("df_stock_order"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_sum_cost.alias("df_sum_cost"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_out_of_stock.alias("df_out_of_stock"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_low_stock_impact.alias("df_low_stock_impact"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_potential_low_stock_impact.alias("df_potential_low_stock_impact"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_no_stock_instances.alias("df_no_stock_instance"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .join(df_weeks_supply.alias("df_weeks_supply"),["YR_WK_NUM","STORE_KEY","PROD_KEY"]) \
        .select(
            "df_sum.*",
            "df_stock_level.INVENTORY_ON_HAND_QTY_END_WK",
            "df_stock_order.INVENTORY_ON_ORDER_QTY_END_WK",
            "df_sum_cost.total_cost_week",
            "df_out_of_stock.out_of_stock_count",
            "df_low_stock_impact.total_low_stock_impact",
            "df_potential_low_stock_impact.potential_low_stock_impact",
            "df_no_stock_instance.no_stock_instances",
            "df_weeks_supply.weeks_supply"
        )
    df_final.show()



# # Write the Sparksql file to the S3 bucket
    df_final.repartition(1).write.mode("overwrite").option("csv").parquet(f"s3://aws-project-output-24/date={date_str}")
