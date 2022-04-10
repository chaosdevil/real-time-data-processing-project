import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ast import literal_eval


os.environ["PYSPARK_PYTHON"] = "/opt/cloudera/parcels/Anaconda/bin/python"
os.environ["JAVA_HOME"] = "/usr/java/jdk1.8.0_232-cloudera/jre/"
os.environ["SPARK_HOME"] = "/opt/cloudera/parcels/SPARK2-2.3.0.cloudera2-1.cdh5.13.3.p0.316101/lib/spark2/"
os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
sys.path.insert(0, os.environ["PYLIB"] + "/py4j-0.10.6-src.zip")
sys.path.insert(0, os.environ["PYLIB"] + "/pyspark.zip")


# get total cost
def get_total_cost(items):
    items = literal_eval(items)
    total_cost = 0
    for item in items:
        total_cost += item["unit_price"] * item["quantity"]
    # total_cost = round(total_cost, 2)
    return total_cost

# get total items
def get_total_items(items):
    items = literal_eval(items)
    total_items = 0
    for item in items:
        total_items += item["quantity"]
    return total_items

# if that order is ORDER or RETURN
def type_order(category):
    if category == "ORDER":
        return 1
    return 0

def type_return(category):
    if category == "RETURN":
        return 1
    return 0

if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: spark-submit spark-streaming.py <hostname> <port> <topic>")
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

    spark = SparkSession  \
	    .builder  \
	    .appName("RetailDataAnalysis")  \
	    .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    bootstrap_server = host + ":" + port

    lines = spark  \
	    .readStream  \
	    .format("kafka")  \
	    .option("kafka.bootstrap.servers", bootstrap_server)  \
	    .option("subscribe", topic)  \
	    .load()

    schema = StructType() \
            .add("invoice_no", StringType()) \
            .add("country", StringType()) \
            .add("timestamp", TimestampType()) \
            .add("type", StringType()) \
            .add("items", StringType())

    raw_data = lines.selectExpr("cast(value as string)").select(from_json("value", schema).alias("temp")).select("temp.*")

    # create user-defined functions
    total_cost = udf(lambda items: get_total_cost(items))
    total_quantity = udf(lambda items: get_total_items(items))
    is_order = udf(lambda types: type_order(types))
    is_return = udf(lambda types: type_return(types))

    new_df = raw_data
    new_df = new_df.withColumn("total_cost", total_cost("items")) \
            .withColumn("total_items", total_quantity("items")) \
            .withColumn("is_order", is_order("type")) \
            .withColumn("is_return", is_return("type"))


    # create kafka dataframe
    kafkaDF = new_df.select(["invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return"])
    kafkaDF = kafkaDF.withColumn("total_cost", when(kafkaDF.is_order == 1, kafkaDF.total_cost).otherwise(-kafkaDF.total_cost))

    # streaming raw data
    query0 = kafkaDF.select(["invoice_no", "country", "timestamp", "total_cost", "total_items", "is_order", "is_return"])


    # create time-based KPI
    query1 = kafkaDF.select(["timestamp", "invoice_no", "total_cost", "is_order", "is_return"])
    query1 = query1.withWatermark("timestamp", "1 minute").groupBy(window("timestamp", "1 minute")) \
            .agg(round(sum("total_cost"), 2).alias("total_sales_volume"), count("invoice_no").alias("OPM"), \
                round(sum("is_return") / (sum("is_order") + sum("is_return")), 2).alias("rate_of_return"), \
                round(sum("total_cost") / count("invoice_no"), 2).alias("average_transaction_size"))


    # create time-and-country based KPI
    query2 = kafkaDF.select(["timestamp", "invoice_no", "country", "total_cost", "is_order", "is_return"])
    query2 = query2.withWatermark("timestamp", "1 minute").groupBy(window("timestamp", "1 minute"), "country") \
            .agg(round(sum("total_cost"), 2).alias("total_sales_volume"), count("invoice_no").alias("OPM"), \
                round(sum("is_return") / (sum("is_order") + sum("is_return")), 2).alias("rate_of_return"))


    # write stream data
    query0 = query0.writeStream \
        .format("console") \
        .outputMode("append") \
        .option("truncate", "false") \
        .trigger(processingTime="1 minute") \
        .start()

    query1 = query1.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("path", "/user/ec2-user/real-time-project/warehouse/op1") \
        .option("checkpointLocation", "hdfs:///user/ec2-user/real-time-project/warehouse/checkpoints1") \
        .trigger(processingTime="1 minute") \
        .start()

    query2 = query2.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("truncate", "false") \
        .option("path", "/user/ec2-user/real-time-project/warehouse/op2") \
        .option("checkpointLocation", "hdfs:///user/ec2-user/real-time-project/warehouse/checkpoints2") \
        .trigger(processingTime="1 minute") \
        .start()

    query0.awaitTermination()
    query1.awaitTermination()
    query2.awaitTermination()
