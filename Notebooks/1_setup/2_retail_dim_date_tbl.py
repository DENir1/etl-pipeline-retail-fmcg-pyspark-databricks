# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ####Define start and end dates

# COMMAND ----------

# start date and end date
start_date = '2024-01-01'
end_date   = '2025-12-01'

# COMMAND ----------


df = spark.sql(f'''
               select explode(sequence(to_date('{start_date}','yyyy-MM-dd'), to_date('{end_date}','yyyy-MM-dd'), interval 1 month)) as month_start_date
            ''')


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####adding analytical column

# COMMAND ----------

df = (
    df.withColumn("date_key",year(col("month_start_date")).cast(IntegerType()))\
        .withColumn("year",year(col("month_start_date")).cast(IntegerType()))\
            .withColumn("month_name",date_format(col("month_start_date"),"MMMM"))\
                .withColumn("month_short_name",date_format(col("month_start_date"),"MMM"))\
                    .withColumn("quarter",concat(lit("Q"),quarter(col("month_start_date"))))\
                        .withColumn("year_quarter",concat(col("year"),lit("-Q"),quarter(col("month_start_date"))))
)

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####save as table

# COMMAND ----------

df.write.format("delta")\
    .mode("overwrite")\
        .saveAsTable("fmcg.gold.dim_date") #Am@jlsncm$6

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from fmcg.gold.dim_date