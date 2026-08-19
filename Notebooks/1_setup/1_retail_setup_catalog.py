# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create catalog if not exists fmcg;
# MAGIC use catalog fmcg;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### create gold schema for both child and parent company
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create schema if not exists fmcg.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases from fmcg

# COMMAND ----------

# MAGIC %md
# MAGIC ####create bronze and silver schema for child company

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists fmcg.bronze;
# MAGIC create schema if not exists fmcg.silver;