# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/retail_project/1_setup/3_utilities/

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.fs.ls(f"s3://child-sportsbar/orders/landing")

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","orders","Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)

# COMMAND ----------

base_path = f's3://child-sportsbar/{data_source}'

landing_path = f'{base_path}/landing/'
processed_path = f'{base_path}/processed/'

print('base_path', base_path)
print('landing_path', landing_path)
print('processed_path', processed_path)

#define table

bronze_table = f'{catalog}.{bronze_schema}.{data_source}'
silver_table = f'{catalog}.{silver_schema}.{data_source}'
gold_table = f'{catalog}.{gold_schema}.sb_fact_{data_source}'

print('bronze_table', bronze_table)
print('silver_table', silver_table)
print('gold_table', gold_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ####data extraction

# COMMAND ----------

df = spark.read.format("csv")\
            .option("header",True)\
                .option("inferschema",True)\
                    .load(landing_path)\
                        .withColumn("read_timestamp", current_timestamp())\
                            .select("*", "_metadata.file_name","_metadata.file_size")

print("total_rows", df.count())

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####bronze writing

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from  fmcg.bronze.orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete
# MAGIC from fmcg.bronze.orders
# MAGIC where read_timestamp in (
# MAGIC select read_timestamp
# MAGIC from
# MAGIC (
# MAGIC select order_id, order_placement_date, customer_id, product_id, order_qty, read_timestamp, file_name, file_size,
# MAGIC dense_rank() over(partition by order_id, order_placement_date, customer_id, product_id, order_qty order by read_timestamp) rn
# MAGIC from fmcg.bronze.orders
# MAGIC )
# MAGIC where rn>1
# MAGIC group by read_timestamp
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from  fmcg.bronze.orders
# MAGIC
# MAGIC

# COMMAND ----------

delta_table_bronze = DeltaTable.forName(spark, bronze_table)

delta_table_bronze.alias("t").merge(
    df.select("file_name").distinct().alias("s"),
    "t.file_name = s.file_name"
).whenMatchedDelete().execute()


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from  fmcg.bronze.orders

# COMMAND ----------

print('before loading', spark.read.table(bronze_table).count())

df.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("append")\
            .saveAsTable(bronze_table)

print('after loading', spark.read.table(bronze_table).count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating staging table in bronze schema to hold the latest data

# COMMAND ----------

spark.sql(f'drop table if exists {catalog}.{bronze_schema}.staging_{data_source}')

# COMMAND ----------

df.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("overwrite")\
            .saveAsTable(f'{catalog}.{bronze_schema}.staging_{data_source}')

# COMMAND ----------


print("total records", spark.read.table(f'{catalog}.{bronze_schema}.staging_{data_source}').count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####silver

# COMMAND ----------

df_orders = spark.read.table(f'{catalog}.{bronze_schema}.staging_{data_source}')

print('df_orders', df_orders.count())
display(df_orders.limit(5))

# COMMAND ----------

df_orders = df_orders.filter(col("order_qty").isNotNull())

# COMMAND ----------

df_orders = df_orders.withColumn("customer_id",
                                  when(col("customer_id").rlike('^[0-9]+$'),col("customer_id"))\
                                      .otherwise('999999').cast("string"))

# COMMAND ----------

df_orders.select(col("customer_id")).distinct().display()

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date",
                                 regexp_replace(col("order_placement_date"), r'^[a-z-A-Z]+,\s*', '')
                                  )

display(df_orders)

# COMMAND ----------

df_orders = df_orders.withColumn("order_placement_date",
                     coalesce(
                         try_to_date(col("order_placement_date"),'dd-MM-yyyy'),
                         try_to_date(col("order_placement_date"),'MMMM dd, yyyy'),
                         try_to_date(col("order_placement_date"),'yyyy/MM/dd'),
                         try_to_date(col("order_placement_date"),'dd/MM/yyyy')
                         ))


# COMMAND ----------

df_orders.select(col("order_placement_date")).distinct().display()

# COMMAND ----------

df_orders = df_orders.dropDuplicates(["order_id",\
                                     "order_placement_date",\
                                     "customer_id",\
                                         "product_id",\
                                            "order_qty"])
                                            

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

spark.sql(f'select * from {silver_table}').orderBy(col("order_placement_date").desc()).limit(2).display()

# COMMAND ----------

print('before upsert',spark.sql(f'select * from {silver_table}').count())

# COMMAND ----------

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta")\
        .option("delta.enableChangeDataFeed", True)\
            .option("mergeschema", True)\
                .mode("overwrite")\
                    .saveAsTable(silver_table)

else:
    delta_sliver = DeltaTable.forName(spark, (silver_table))

    delta_sliver.alias("t").merge(
        df_joined.alias("s"),
        "t.order_id == s.order_id and\
        t.order_placement_date == s.order_placement_date and\
        t.customer_id == s.customer_id and\
        t.product_id == s.product_id and\
        t.product_code == s.product_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()


# COMMAND ----------

print('after upsert', spark.sql(f'select * from {silver_table}').count())

# COMMAND ----------

spark.sql(f'drop table if exists {catalog}.{silver_schema}.staging_{data_source}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### creating cleaned and transformed staging table in silver schema

# COMMAND ----------

df_joined.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("overwrite")\
            .saveAsTable(f'{catalog}.{silver_schema}.staging_{data_source}')


# COMMAND ----------

# MAGIC %md
# MAGIC ####gold

# COMMAND ----------

df_gold = spark.sql(f'select order_id, order_placement_date as date, customer_id as customer_code, product_id, order_qty as sold_quantity, product_code from {catalog}.{silver_schema}.staging_{data_source}')

display(df_gold.limit(2))


# COMMAND ----------

spark.sql(f'select * from {gold_table}').limit(2).display()

# COMMAND ----------

if not (spark.catalog.tableExists(gold_table)):
    df_gold.write.format("delta")\
        .option("delta.enableChangeDataFeed", True)\
            .mode("overwrite")\
                .option("mergeschema", True)\
                    .saveAsTable(gold_table)
else:
    delta_gold = DeltaTable.forName(spark, gold_table)

    delta_gold.alias("t").merge(
        df_gold.alias("s"),
        "t.order_id == s.order_id and\
        t.date == s.date and\
        t.customer_code == s.customer_code and\
        t.product_code == s.product_code"
    ).whenMatchedUpdateAll(
    ).whenNotMatchedInsertAll(
    ).execute()

# COMMAND ----------

df_child = spark.sql(f'select * from {catalog}.{gold_schema}.sb_fact_{data_source}')

# COMMAND ----------

df_child = df_child.withColumn("date_monthly", date_format(col("date"),"yyyy-MM-01").cast("date"))

display(df_child)

# COMMAND ----------

df_child = df_child.groupBy(col("customer_code"),col("product_code"),col("date_monthly"))\
    .agg(sum(col("sold_quantity")).alias("sold_quantity"))\
    .select("date_monthly","product_code","customer_code","sold_quantity")\
    .orderBy(col("date_monthly").desc())


display(df_child)



# COMMAND ----------

delta_table_gold = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.fact_{data_source}')

delta_table_gold.alias("t").merge(
    df_child.alias("s"),
    "t.date == s.date_monthly and\
    t.product_code == s.product_code and\
    t.customer_code == s.customer_code"
).whenMatchedUpdate(
    set = {
        "product_code": "s.product_code",
        "customer_code": "s.customer_code",
        "sold_quantity": "s.sold_quantity"
        }
).whenNotMatchedInsert(
    values={
        "date": "s.date_monthly",
        "product_code": "s.product_code",
        "customer_code": "s.customer_code",
        "sold_quantity": "s.sold_quantity"
    }
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select date,count(*)
# MAGIC from fmcg.gold.fact_orders
# MAGIC group by date
# MAGIC order by date desc

# COMMAND ----------

files = dbutils.fs.ls(landing_path)

for file in files:
    dbutils.fs.mv(file.path, f'{processed_path}/{file.name}', True)