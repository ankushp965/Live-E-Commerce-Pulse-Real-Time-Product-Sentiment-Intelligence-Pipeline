# Databricks notebook source
spark.conf.set(
    "STORAGE ACCOUNT",
    "STORAGE KEY")

# COMMAND ----------

from pyspark.sql.functions import col

gold_df = spark.read.format("delta").option("header",True).option("inferSchema", True).load("abfss://ecommerce@cdacprojects.dfs.core.windows.net/silver_layer/amazon_products")

# COMMAND ----------

# Derive discount columns
gold_df = gold_df.withColumn(
    "discount_amount",
    (col("product_price") - col("product_minimum_offer_price"))
)

# COMMAND ----------

display(gold_df)  # Ensure 'gold_df' is loaded as a Delta table, not as Parquet

# COMMAND ----------

from pyspark.sql.functions import when
gold_df = gold_df.withColumn(
    "discount_percentage",
    when(col("product_minimum_offer_price") > 0,
           ((col("product_price") - col("product_minimum_offer_price")) / col("product_price")) * 100
          ).otherwise(0)
)

# COMMAND ----------

# Add price categories
gold_df = gold_df.withColumn(
    "price_category",
     when(col("product_price") < 100, "Low")
     .when((col("product_price") >= 100) & (col("product_price") < 300), "Medium")
     .otherwise("High")
)

# COMMAND ----------

gold_df.show()

# COMMAND ----------

gold_df.count()

# COMMAND ----------

# Save Gold
path = "abfss://ecommerce@cdacprojects.dfs.core.windows.net/gold_layer/amazon_products"
gold_df.write.format("delta").mode("overwrite").save(path)

# COMMAND ----------

#INSIGHTS FROM GOLD_LAYER
gold_df.createOrReplaceTempView("gold_df")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_title, sales_volume
# MAGIC             FROM gold_df
# MAGIC             ORDER BY sales_volume DESC
# MAGIC             LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AVG(product_price) AS avg_price, AVG(product_num_ratings) AS avg_ratings
# MAGIC FROM gold_df

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_prime, AVG(sales_volume) AS avg_sales, AVG(product_price) AS avg_price
# MAGIC FROM gold_df
# MAGIC GROUP BY is_prime;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT is_best_seller, AVG(product_num_ratings) AS avg_reviews, AVG(sales_volume) AS avg_sales
# MAGIC FROM gold_df
# MAGIC GROUP BY is_best_seller;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(DISTINCT product_title) AS unique_product_titles
# MAGIC FROM gold_df
# MAGIC

# COMMAND ----------

