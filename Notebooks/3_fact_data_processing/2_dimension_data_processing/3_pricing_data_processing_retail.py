# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run /Workspace/retail_project/1_setup/3_utilities

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","gross_price","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)



# COMMAND ----------

dbutils.fs.ls(f"s3://child-sportsbar/{data_source}")

base_path = f"s3://child-sportsbar/{data_source}/*.csv"

print(base_path)

# COMMAND ----------

df = spark.read.format("csv")\
        .option("header", True)\
            .option("inferSchema",True)\
                .load(base_path)\
                    .withColumn("read_timestamp", current_timestamp())\
                        .select("*","_metadata.file_name","_metadata.file_size")\


display(df)

print('total records', df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####bronze

# COMMAND ----------

df.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("overwrite")\
            .saveAsTable(f'{catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

df_bronze = spark.sql(f' select * from {catalog}.{bronze_schema}.{data_source}')

# COMMAND ----------

display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ####silver

# COMMAND ----------

print('records before duplicated dropped', df_bronze.count())
df_silver = df_bronze.dropDuplicates()
print('records after duplicated dropped', df_silver.count())

# COMMAND ----------

df_silver = df_silver.withColumn("month",
                     coalesce(
                        try_to_date(col("month"),"yyyy-MM-dd"),
                        try_to_date(col("month"),"yyyy/MM/dd"),
                        try_to_date(col("month"),"dd/MM/yyy"),
                        try_to_date(col("month"),"dd-MM-yyyy")
                        )
                     )


display(df_silver)

# COMMAND ----------


df_silver = df_silver.withColumn(
                     "gross_price",when(col("gross_price").rlike('^[a-zA-Z_]*$'),'0')\
                    .otherwise(col("gross_price")))\
          .withColumn("gross_price", when(col("gross_price").cast(IntegerType())<0, -1*col("gross_price").cast(IntegerType()))\
                                .otherwise(col("gross_price").cast(IntegerType())))


display(df_silver)


# COMMAND ----------

df_products = spark.sql(f'select * from fmcg.silver.products')

df_joined = df_silver.alias("s").join(df_products.select(col("product_id").cast(IntegerType()).alias("product_id"), 
                                                            col("product_code")).alias("p"),
                                      col("p.product_id") == col("s.product_id"),"inner")\
                                 .select(
                                    col("s.product_id"),
                                    col("p.product_code"),
                                    col("s.gross_price"),
                                    col("s.month"),
                                    col("s.read_timestamp"),
                                    col("s.file_name"),
                                    col("s.file_size"))

display(df_joined)


# COMMAND ----------

display(df_joined)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fmcg.gold.dim_gross_price

# COMMAND ----------

df_joined = df_joined.withColumn("year", year(col("month")))

display(df_joined)

# COMMAND ----------

df_joined = df_joined.withColumn("yrs_wise_grs_price",
sum(col("gross_price")).over(Window.partitionBy(col("year"), col("product_code")).orderBy(col("year").desc())))

df_joined = df_joined.dropDuplicates()

display(df_joined)

# COMMAND ----------

df_joined.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .option("mergeSchema",True)\
            .mode("overwrite")\
                .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####gold

# COMMAND ----------

df_silver = spark.read.table(f'{catalog}.{silver_schema}.{data_source}')

# COMMAND ----------

df_gold = df_silver.select("product_code","yrs_wise_grs_price","year").distinct()

# COMMAND ----------

display(df_gold)

# COMMAND ----------

df_gold.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
        .mode("overwrite")\
            .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")


# COMMAND ----------

delta_table_gross_price = DeltaTable.forName(spark,f'{catalog}.{gold_schema}.dim_{data_source}')

df_gold_gross = spark.read.table(f'{catalog}.{gold_schema}.sb_dim_{data_source}')

df_gold_gross.printSchema()


# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from fmcg.gold.dim_gross_price
# MAGIC union
# MAGIC select count(*)
# MAGIC from fmcg.gold.sb_dim_gross_price;

# COMMAND ----------

delta_table_gross_price.alias("t").merge(
df_gold_gross.alias("s"),
"t.product_code == s.product_code and\
 t.year == s.year"
).whenMatchedUpdate(
set = {
    "price_inr":"s.yrs_wise_grs_price"
}
).whenNotMatchedInsert(
values = {
"product_code":"s.product_code",
"price_inr":"s.yrs_wise_grs_price",
"year":"s.year"
}
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from fmcg.gold.dim_gross_price