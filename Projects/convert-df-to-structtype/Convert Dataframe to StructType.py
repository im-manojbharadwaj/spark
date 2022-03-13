# Databricks notebook source
# MAGIC %md
# MAGIC <h1>FUNCTION TO CONVERT PYSPARK DATAFRAME TYPE TO PYSPARK STRUCTTYPE</h1>
# MAGIC <h4>
# MAGIC   The reason we convert is that we can use the schema to impose on other dataframes.<br>
# MAGIC   Cell 4 to Cell 19 is pure work of fixing the schema when data is received in json format. You can ignore these steps
# MAGIC </h4>
# MAGIC <h7>- dr.dataspark</h6>

# COMMAND ----------

# drivers
import pyspark
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.types import StructType
import json

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>
# MAGIC   Below is the core function that converts the dataframe to struct-type
# MAGIC </h4>

# COMMAND ----------

# the function that converts the pyspark dataframe into struct type
def convert_to_struct(df):
    schema_json = df.schema.json()
    #schema_json = schema_json.replace('"type":"null", "type":"string"').replace('"type":"integer", "type":"string"')
    new_schema = StructType.fromJson(json.loads(schema_json))
    return new_schema

# COMMAND ----------

# MAGIC %md
# MAGIC <h4>
# MAGIC   Regular data engineering processing of loading data from json and fixing schema
# MAGIC </h4>

# COMMAND ----------

# load the data from json file
ordersDf = spark.read.format("json")\
                         .option("inferSchema", "true")\
                         .option("multiLine", "true")\
                         .load('/FileStore/tables/data.json')

# COMMAND ----------

# uncomment below code to see the schema
# ordersDf.printSchema()

# COMMAND ----------

# exploding the orders fields
parseOrdersDf = ordersDf.select(explode(ordersDf.datasets).alias("orders"))

# COMMAND ----------

# uncomment below code to see the schema
# parseOrdersDf.printSchema()

# COMMAND ----------

# making all the fields to come under root
parseOrdersDf = parseOrdersDf.select("orders.*")

# COMMAND ----------

# uncomment below code to see the schema
# parseOrdersDf.printSchema()

# COMMAND ----------

# exploding orderDetails
new_parseOrdersDf = parseOrdersDf.select("customerId", "orderDate", "orderId", "shipmentDetails", explode(parseOrdersDf.orderDetails).alias("orderDetails"))

# COMMAND ----------

# uncomment below code to see the schema
# new_parseOrdersDf.printSchema()

# COMMAND ----------

# uncomment below line to check the dataframe
# display(new_parseOrdersDf)

# COMMAND ----------

# flattening shipment column and dropping the struct type field
final_df = new_parseOrdersDf.withColumn("city", col("shipmentDetails.city"))\
                            .withColumn("country", col("shipmentDetails.country"))\
                            .withColumn("state", col("shipmentDetails.state"))\
                            .withColumn("street", col("shipmentDetails.street"))\
                            .drop("shipmentDetails")

# COMMAND ----------

# uncomment below line to check the dataframe
# display(final_df)

# COMMAND ----------

# flattening orderDetails column and dropping the struct type field
new_final_df = final_df.withColumn("productId", col("orderDetails.productId"))\
                            .withColumn("quality", col("orderDetails.quantity"))\
                            .withColumn("sequence", col("orderDetails.sequence"))\
                            .withColumn("totalPrice", col("orderDetails.totalPrice"))\
                            .drop("orderDetails")

# COMMAND ----------

# uncomment below line to check the dataframe
# display(new_final_df)

# COMMAND ----------

# flattening totalPrice column and dropping the struct type field
final_df = new_final_df.withColumn("grossPrice", col("totalPrice.gross"))\
.withColumn("netPrice", col("totalPrice.net"))\
.withColumn("tax", col("totalPrice.tax"))\
.drop("totalPrice")

# COMMAND ----------

# uncomment below line to check the dataframe
# display(final_df)

# COMMAND ----------

# verifying the schema information of the final dataframe
final_df.printSchema()

# COMMAND ----------

# checking the type
type(final_df)

# COMMAND ----------

# calling the function to convert the pyspark dataframe to pyspark struct type
schema = convert_to_struct(final_df)

# COMMAND ----------

# checking the type
type(schema)
