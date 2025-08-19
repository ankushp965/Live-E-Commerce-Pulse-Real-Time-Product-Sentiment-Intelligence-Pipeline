# Databricks notebook source
spark.conf.set(
    "STORAGE ACCOUNT",
    "STORAGE ACCOUNT KEY")

# COMMAND ----------

bronze_df = spark.read.option("header", "true").option("inferSchema", True).csv("abfss://ecommerce@cdacprojects.dfs.core.windows.net/raw/amazon_products.csv")

# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, lit, col, when
from pyspark.sql.types import FloatType, IntegerType
import html

# Select only chosen columns
silver_df = bronze_df.select(
    "asin",
    "product_price",
    "sales_volume",
    "is_best_seller",
    "is_prime",
    "product_num_ratings",
    "product_title",
    "product_minimum_offer_price")

# COMMAND ----------

# Clean product_price & min_offer_price
price_cols = ["product_price", "product_minimum_offer_price"]
for col_name in price_cols:
    silver_df = silver_df.withColumn(col_name, regexp_replace(col(col_name), "[$,]", ""))
    silver_df = silver_df.withColumn(col_name, col(col_name).cast(FloatType()))
    silver_df = silver_df.withColumn(col_name, when(col(col_name).isNull(), lit(0.0)).otherwise(col(col_name)))


# COMMAND ----------

# Clean sales_volume (remove text, handle K+, +)
silver_df = silver_df.withColumn("sales_volume", regexp_replace(col("sales_volume"), " bought in past month", ""))
silver_df = silver_df.withColumn("sales_volume", regexp_replace(col("sales_volume"), "\\+", ""))
silver_df = silver_df.withColumn("sales_volume", regexp_replace(col("sales_volume"), "K", "000"))
silver_df = silver_df.withColumn("sales_volume", col("sales_volume").cast(IntegerType()))
silver_df = silver_df.withColumn("sales_volume", when(col("sales_volume").isNull(), lit(0)).otherwise(col("sales_volume")))


# COMMAND ----------

# Fix ratings
silver_df = silver_df.withColumn("product_num_ratings", col("product_num_ratings").cast(IntegerType()))
silver_df = silver_df.withColumn("product_num_ratings", when(col("product_num_ratings").isNull(), lit(0)).otherwise(col("product_num_ratings")))


# COMMAND ----------

# Boolean cleanup
silver_df = silver_df.withColumn("is_best_seller", col("is_best_seller").cast("boolean"))
silver_df = silver_df.withColumn("is_prime", col("is_prime").cast("boolean"))


# COMMAND ----------

# Decode product_title (HTML entities)
decode_html_udf = udf(lambda x: html.unescape(x) if x else x)
silver_df = silver_df.withColumn("product_title", decode_html_udf(col("product_title")))


# COMMAND ----------


# Save Silver
path = "abfss://ecommerce@cdacprojects.dfs.core.windows.net/silver_layer/amazon_products"
silver_df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

silver_df.show()

# COMMAND ----------

