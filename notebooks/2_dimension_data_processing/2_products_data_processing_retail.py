# Databricks notebook source
from pyspark.sql.functions import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/retail_project/1_setup/3_utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","products","Data Source")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")

data_source = dbutils.widgets.get("data_source")

print(catalog, data_source)


# COMMAND ----------

dbutils.fs.ls(f's3://child-sportsbar/{data_source}/')

# COMMAND ----------

base_path = f's3://child-sportsbar/{data_source}/*.csv'

# COMMAND ----------

df =(
    spark.read.format("csv")\
        .option("header", True)\
        .option("inferschema", True)\
        .load(base_path)\
            .withColumn("read_timestamp", current_timestamp())\
         .select("*", "_metadata.file_name", "_metadata.file_size")
)


display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ####bronze

# COMMAND ----------

df.write.format("delta")\
    .option("delta.enableChangeDataFeed", True)\
        .mode("overwrite")\
        .option("mergeSchema", True)\
            .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####silver

# COMMAND ----------

df_bronze = spark.read.table(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Transformation
# MAGIC
# MAGIC ####Dropduplicates

# COMMAND ----------

print('rows before duplicates dropped', df_bronze.count())
df_silver = df_bronze.dropDuplicates()
print('rows after duplocates dropped', df_silver.count())

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ####title case fix

# COMMAND ----------

df_silver = df_silver.withColumn("category", when(col("category").isNull(), None)
                                    .otherwise(initcap(col("category"))))


display(df_silver)


# COMMAND ----------

# MAGIC %md 
# MAGIC ####spelling mistake for protien

# COMMAND ----------

df_silver = df_silver.withColumn("category", regexp_replace(col("category"), "(?i)protien", "Protein"))\
                    .withColumn("product_name", regexp_replace(col("product_name"), "(?i)protien", "Protein"))

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Add divison column

# COMMAND ----------

df_silver = df_silver.withColumn("division", when(col("category").isin('Energy Bars','Protein Bars'),'Nutrition Bar')
                                            .when(col("category") == 'Granola & Cereals','Breakfast Foods')
                                            .when(col("category") == 'Recovery Dairy','Dairy & Recovery')
                                            .when(col("category") == 'Healthy Snacks','Healthy Snacks')
                                            .when(col("category") == 'Electrolyte Mix','Hydration & Electrolytes')
                                            .otherwise('other')
                                            )

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### add variant column

# COMMAND ----------

df_silver = df_silver.withColumn("variant", regexp_extract(col("product_name"), r"\((.*?)\)", 1))

display(df_silver)

# COMMAND ----------

df_silver = df_silver.withColumn("product_code", sha2(col("product_name"),256))\
                     .withColumn("product_id", when(col("product_id").rlike("^[0-9]+$"), col("product_id"))
                                                .otherwise('999999').cast("string"))

display(df_silver)


# COMMAND ----------

df_silver = df_silver.withColumnRenamed("product_name","product")

display(df_silver)

# COMMAND ----------

df_silver = df_silver.select("product_code","division","category","product","variant","product_id",\
                                    "read_timestamp", "file_name", "file_size")

display(df_silver)

# COMMAND ----------

df_silver.write.format("delta")\
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

display(df_silver)

# COMMAND ----------

df_gold = df_silver.select("product_code","division","category","product","variant")

# COMMAND ----------

df_gold.write.format("delta")\
        .option("delta.enableChangeDataFeed",True)\
                  .mode("overwrite")\
                .saveAsTable(f'{catalog}.{gold_schema}.sb_dim_{data_source}')


# COMMAND ----------

# MAGIC %md
# MAGIC ####merge data with parent 

# COMMAND ----------

delta_table_prod = DeltaTable.forName(spark, f'{catalog}.{gold_schema}.dim_{data_source}')

df_child_products = (
                spark.read.table(f'{catalog}.{gold_schema}.sb_dim_{data_source}')\
                                .select("product_code","division","category","product","variant")
)


display(df_child_products)

# COMMAND ----------

# MAGIC %md
# MAGIC ####upsert

# COMMAND ----------

delta_table_prod.alias("t").merge(
    df_child_products.alias("s"),
    "t.product_code=s.product_code"
).whenMatchedUpdate(
    set={
        "division" : "s.division",
        "category" : "s.category",
        "product" : "s.Variant",
        "variant" : "s.Variant"
    }
    ).whenNotMatchedInsert(
     values = {
        "product_code" : "s.product_code",
        "division" : "s.division",
        "category" : "s.category",
        "product" : "s.Variant",
        "variant" : "s.Variant"
                }
        ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from fmcg.gold.dim_products

# COMMAND ----------

