# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import *


# COMMAND ----------

# MAGIC %run /Workspace/retail_project/1_setup/3_utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### widgets creation

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")


catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)

# COMMAND ----------

base_path = f's3://child-sportsbar/{data_source}/*.csv'

print(base_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ####dbutils explore

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.ls(f's3://child-sportsbar/orders/landing')

# COMMAND ----------

# MAGIC %md
# MAGIC ####bronze

# COMMAND ----------

df = (
    spark.read.format("csv")\
        .option("header",True)\
        .option("inferschema", True)\
        .load(base_path)\
        .withColumn("read_timestamp",current_timestamp())\
        .select("*","_metadata.file_name","_metadata.file_size")
)

display(df)


# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
    .option("delta.enableChangeDataFeed",True)\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ####silver

# COMMAND ----------

df_bronze = spark.sql(f'select * from {catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Transformation
# MAGIC

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1.check duplicates

# COMMAND ----------

df_bronze.groupBy(col("customer_id"),col("customer_name"),col("city"))\
        .count()\
        .sort(col("count").desc())\
        .filter(col("count")>1)\
         .display()

# COMMAND ----------

print('rows before duplicates dropped', df_bronze.count())
df_silver = df_bronze.dropDuplicates()
print('rows after duplicates dropped', df_silver.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 2.extra spaces in values

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver.filter(trim(col("customer_name"))== col("customer_name")).display()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_name",trim(col("customer_name")))\
                    .withColumn("city",trim(col("city")))

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3.correct city name

# COMMAND ----------

df_silver.select(col("city"))\
        .distinct()\
            .display()

# COMMAND ----------

city_mapping = {
   
    "Bengalore": "Bengaluru",
    "Bengaluruu": "Bengaluru",
    
    "Hyderabadd": "Hyderabad",
    "Hyderbad": "Hyderabad",
    
    "NewDelhee": "New Delhi",
    "NewDelhi": "New Delhi",
    "NewDheli":"New Delhi"
}

df_silver = df_silver.replace(city_mapping, subset=["city"])

display(df_silver)

# COMMAND ----------

df_silver.select(col("city"))\
        .distinct()\
        .display()

# COMMAND ----------

allowed = ['Bengaluru', 'Hyderabad', 'New Delhi']

df_silver = df_silver.withColumn("city", when(col("city").isNull(),None)\
                                         .when(col("city").isin(allowed),col("city"))\
                                         .otherwise(None))
display(df_silver)


# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.fix table casing issue

# COMMAND ----------

#sanity check

df_silver.select(col("customer_name"))\
        .distinct()\
        .display()


# COMMAND ----------

df_silver = df_silver.withColumn("customer_name", when(col("customer_name").isNull(), None)\
                                                    .otherwise(initcap(col("customer_name"))))

display(df_silver)

# COMMAND ----------

# sanity check

df_silver.select(col("customer_name"))\
            .distinct()\
                .sort(col("customer_name").asc())\
                .display()

# COMMAND ----------

df_silver.filter(col("city").isNull())\
        .display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####customer city fix

# COMMAND ----------

data = [
(789403,	'New Delhi'),
(789420,	'Bengaluru'),
(789521,	'Hyderabad'),
(789603,	'Hyderabad')
]

df_fix = spark.createDataFrame(data,["customer_id", "fixed_city"])

display(df_fix)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

display(df_silver)

# COMMAND ----------

df_silver = (
                df_silver.join(df_fix, df_silver.customer_id == df_fix.customer_id, "left")\
                        .withColumn("city", coalesce(df_silver.city,df_fix.fixed_city))
                        .drop(col("fixed_city"))
                        .drop(df_fix.customer_id)
)

display(df_silver)

# COMMAND ----------

df_silver.filter(col("city").isNull()).display()

# COMMAND ----------

df_silver = df_silver.withColumn("customer_id", col("customer_id").cast(StringType()))

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### standardising customer attribute to match parent company data model

# COMMAND ----------

df_silver = df_silver.withColumn("customer", concat_ws(" - ", col("customer_name"), coalesce(col("city"), lit("Unknown"))))\
                    .withColumn("market", lit("India"))\
                        .withColumn("platform", lit("Sportsbar"))\
                            .withColumn("channel", lit("Acquisition"))
                    
display(df_silver)

# COMMAND ----------

df_silver.write.format("delta")\
    .mode("overwrite")\
        .option("delta.enableChangeDataFeed", True)\
            .option("mergeSchema",True)\
            .saveAsTable(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Gold

# COMMAND ----------

df_silver = spark.sql(f'select * from {catalog}.{silver_schema}.{data_source}')


# COMMAND ----------

df_gold = df_silver.select(col("customer_id"),
                           col("customer_name"),
                           col("city"),
                           col("read_timestamp"),
                           col("file_name"),
                           col("file_size"),
                           col("customer"),
                           col("market"),
                           col("platform"),
                           col("channel")
                            )

# COMMAND ----------

df_gold.write.format("delta")\
            .option("delta.enableChangeDataFeed",True)\
                .mode("overwrite")\
                    .saveAsTable(f'{catalog}.{gold_schema}.sb_dim_{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### merging with parent data

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from fmcg.gold.dim_customers

# COMMAND ----------

delta_table_gold = DeltaTable.forName(spark, "fmcg.gold.dim_customers")

df_child_customer = spark.read.table(f'{catalog}.{gold_schema}.sb_dim_{data_source}')\
                        .select(col("customer_id").alias("customer_code"),
                                col("customer"),
                                col("market"),
                                col("platform"),
                                col("channel")
                                )

display(df_child_customer)

# COMMAND ----------

delta_table_gold.alias("target").merge(
        df_child_customer.alias("source"),
         "target.customer_code = source.customer_code"
         ).whenMatchedUpdate(
            set = {"customer" : "source.customer",
                "market" : "source.market",
                "platform" : "source.platform",
                "channel"  : "source.channel"}
    ).whenNotMatchedInsert(
            values = {
                "customer_code" : "source.customer_code",
                "customer" : "source.customer",
            "market" : "source.market",
            "platform" : "source.platform",
            "channel" : "source.channel"}
            ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from fmcg.gold.dim_customers

# COMMAND ----------

