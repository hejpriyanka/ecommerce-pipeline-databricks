# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #### Step 1: Create Catalog
# MAGIC
# MAGIC - 📁 The catalog acts as a **top-level namespace** in Unity Catalog.
# MAGIC - 🧱 It serves as a **container for schemas and tables** related to the ecommerce project.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dataengineer_databricks_ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 2: Create Schema
# MAGIC - 🗂️ The schema is created within the catalog to **organize raw, unprocessed data**.
# MAGIC - 🥉 It represents the **Bronze layer** in the medallion architecture, where data is ingested in its original form.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS dataengineer_databricks_ecommerce.bronze_ecommerce;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Step 3: Create Volume
# MAGIC
# MAGIC - 📦 The volume is created within the Bronze schema to **store raw files** such as CSV, JSON, or Parquet.
# MAGIC - 🧩 It allows you to **manage and access files directly** in Unity Catalog, similar to a file system.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS dataengineer_databricks_ecommerce.bronze_ecommerce.bronze_volume;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Load Raw CSV Data into DataFrame
# MAGIC
# MAGIC - 📥 Reads the **`Online-eCommerce.csv`** file from the Bronze volume using Spark with the header option enabled.
# MAGIC - 🧾 Loads the data into a Spark DataFrame and uses `.display()` to visually inspect the dataset in a tabular format.
# MAGIC

# COMMAND ----------

df = spark.read.option("header", "true").csv("/Volumes/dataengineer_databricks_ecommerce/bronze_ecommerce/bronze_volume/Online-eCommerce.csv")
df.display()
