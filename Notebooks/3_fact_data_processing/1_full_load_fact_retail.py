# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/retail_project/1_setup/3_utilities/
# MAGIC

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.fs.ls('s3://child-sportsbar/orders')

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","orders","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = (f's3://child-sportsbar/{data_source}')
landing_path = (f's3://child-sportsbar/{data_source}/landing')
processed_path = (f's3://child-sportsbar/{data_source}/processed')

bronze_table = f'{catalog}.{bronze_schema}.{data_source}'
silver_table = f'{catalog}.{silver_schema}.{data_source}'
gold_table = f'{catalog}.{gold_schema}.sb_fact_{data_source}'

print(base_path)
print(landing_path)
print(processed_path)
print(bronze_table)
print(silver_table)
print(gold_table)



# COMMAND ----------

#dbutils.fs.mv(processed_path,landing_path,True)

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header", True)\
            .option("inferSchema", True)\
                .load(f'{landing_path}/*.csv')\
                    .withColumn("read_timestamp", current_timestamp())\
                        .select('*', '_metadata.file_name', '_metadata.file_size')



# COMMAND ----------

print("total_rows", df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####writing to bronze schema

# COMMAND ----------

df.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("append")\
            .saveAsTable(bronze_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ####file movement to processed

# COMMAND ----------

#files = dbutils.fs.ls(landing_path)

#for file in files:
   # dbutils.fs.mv(file.path, f'{processed_path}/{file.name}', True)

# COMMAND ----------

# MAGIC %md
# MAGIC ####silver

# COMMAND ----------

df_orders = spark.sql(f'select * from {bronze_table}')

# COMMAND ----------

display(df_orders.limit(2))

# COMMAND ----------

df_orders = df_orders.filter(col("order_qty").isNotNull())

# COMMAND ----------

print(df_orders.count())

# COMMAND ----------

df_orders = df_orders.withColumn("customer_id",
                                  when(col("customer_id").rlike('^[0-9]+$'),col("customer_id"))\
                                      .otherwise('999999')
                                  )

# COMMAND ----------

df_orders.select(col("customer_id")).distinct().display()

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date",
                                 regexp_replace(col("order_placement_date"), r'^[a-z-A-Z]+,\s*', '')
                                  )

display(df_orders)

# COMMAND ----------

df_orders.select("order_placement_date").distinct().display()

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date",
                     coalesce(
                         try_to_date(col("order_placement_date"),'dd-MM-yyyy'),
                         try_to_date(col("order_placement_date"),'MMMM dd, yyyy'),
                         try_to_date(col("order_placement_date"),'yyyy/MM/dd'),
                         try_to_date(col("order_placement_date"),'dd/MM/yyyy')
                         ))


# COMMAND ----------

df_orders.select("order_placement_date").distinct().display()

# COMMAND ----------

df_orders = df_orders.dropDuplicates(["order_id",\
                                     "order_placement_date",\
                                     "customer_id",\
                                         "product_id",\
                                            "order_qty"])
                                            

# COMMAND ----------

display(df_orders.count())

# COMMAND ----------

df_orders.limit(2).display()

# COMMAND ----------

df_orders = df_orders.withColumn("product_id", col("product_id").cast("string"))

display(df_orders.limit(2))

# COMMAND ----------

df_products = spark.table('fmcg.silver.products')

# COMMAND ----------

df_joined = df_orders.join(df_products, df_orders.product_id == df_products.product_id, "inner")\
                        .select(df_orders["*"],df_products["product_code"])



# COMMAND ----------

display(df_joined.limit(2))

# COMMAND ----------

display(df_joined.count())

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta")\
        .option("delta.enableChangeDataFeed", True)\
            .option("mergeSchema",True)\
                .mode("overwrite")\
                    .saveAsTable(silver_table)

else:
    silver_delta = DeltaTable.forName(spark,silver_table)
    silver_delta.alias("t").merge(
        df_joined.alias("s"),
        "t.order_placement_date == s.order_placement_date and\
        t.order_id == s.order_id and\
        t.customer_id == s.customer_id and\
        t.product_id == s.product_id"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from fmcg.silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from fmcg.silver.orders
# MAGIC limit 2

# COMMAND ----------

# MAGIC %md
# MAGIC ####gold
# MAGIC

# COMMAND ----------

df_gold = spark.sql(f'''select order_id, order_placement_date as date, customer_id as customer_code,
                    product_code, order_qty as sold_quantity from {silver_table}''')

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    df_gold.write.format("delta")\
        .option("mergeSchema",True)\
            .option("delta.enableChangeDataFeed",True)\
                .mode("overwrite")\
                    .saveAsTable(gold_table)

else:
    gold_delta = DeltaTable.forName(spark,gold_table)

    gold_delta.alias("t").merge(
        df_gold.alias("s"),
        "t.order_id == s.order_id and\
        t.date == s.date and \
        t.customer_code == s.customer_code and\
        t.product_code == s.product_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fmcg.gold.sb_fact_orders

# COMMAND ----------

df_child = spark.sql(f'select date, product_code, customer_code, sold_quantity from {gold_table}')

# COMMAND ----------

df_child.limit(100).display()

# COMMAND ----------

df_child = df_child.withColumn(
                "monthly_date",date_format(col("date"),"yyyy-MM-01")
)

display(df_child)

# COMMAND ----------


display(df_child.count())

# COMMAND ----------



df_child = df_child.select(col("monthly_date").alias("date").cast("date"),
                col("customer_code"),
                col("product_code"),
                col("sold_quantity")
                )\
    .groupBy(col("date"),col("product_code"),col("customer_code"))\
    .agg(sum(col("sold_quantity")).alias("sold_quantity"))


# COMMAND ----------

# MAGIC %md
# MAGIC ###merge with parent table

# COMMAND ----------

df_parent_delta = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.fact_{data_source}')

df_parent_delta.alias("t").merge(
    df_child.alias("s"),
    "t.date == s.date and\
    t.product_code == s.product_code and\
    t.customer_code == s.customer_code"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct date
# MAGIC from fmcg.gold.fact_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ####file movement to processed folder

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file in files:
  dbutils.fs.mv(file.path, f'{processed_path}/{file.name}', True)