# Databricks notebook source
spark.conf.set("STORAGE ACCOUNT",
    "STORAGE KEY")

# COMMAND ----------

# MAGIC %pip install langdetect

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# Import required libraries
from pyspark.sql.functions import col, regexp_replace, lower, trim, when, lit, udf, to_date
from pyspark.sql.types import StringType, IntegerType
from langdetect import detect, DetectorFactory

silver_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("abfss://ecommerce@cdacprojects.dfs.core.windows.net/bronze_layer_reviews/amazon_reviews.csv")

# COMMAND ----------

#Select only the columns which i require
silver_df = silver_df.select(
    "review_id",
    "review_comment",
    "review_star_rating",
    "review_date",
    "reviewed_product_asin"
)

# COMMAND ----------

silver_df = silver_df.dropDuplicates(["review_id"])


# COMMAND ----------

silver_df = silver_df.withColumn("review_star_rating", col("review_star_rating").cast(IntegerType()))

# keep only valid star ratings (1..5) or null
silver_df = silver_df.filter((col("review_star_rating").between(1,5)))

# COMMAND ----------

silver_df = silver_df.withColumn("clean_date", regexp_replace("review_date", "Reviewed in the United States on ", ""))


# COMMAND ----------

silver_df = silver_df.withColumn("review_date", to_date("clean_date","MMMM d, yyyy"))

# COMMAND ----------

silver_df = silver_df.drop("clean_date")

# COMMAND ----------

# To make langdetect deterministic
DetectorFactory.seed = 0

# UDF to detect language
def detect_lang(text):
    try:
        return detect(text)
    except:
        return "unknown"

detect_lang_udf = udf(detect_lang, StringType())


# COMMAND ----------

# 1. Handle null comments
silver_df = silver_df.dropna(subset=["review_comment"])

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# 2. Remove HTML tags, special chars, emojis, numbers
silver_df = silver_df.withColumn(
    "review_comment_clean",
    regexp_replace(col("review_comment"), "<.*?>", "")   # remove HTML tags
)
silver_df = silver_df.withColumn(
    "review_comment_clean",
    regexp_replace(col("review_comment_clean"), "[^a-zA-Z\s]", "")   # keep only alphabets & spaces
)

# COMMAND ----------

# 3. Lowercasing and trimming
silver_df = silver_df.withColumn("review_comment_clean", lower(trim(col("review_comment_clean"))))


# COMMAND ----------

# 4. Detect language
silver_df = silver_df.withColumn("lang", detect_lang_udf(col("review_comment_clean")))


# COMMAND ----------

# 5. Keep only English reviews
silver_df = silver_df.filter(col("lang") == "en")


# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_path = "abfss://ecommerce@cdacprojects.dfs.core.windows.net/silver_layer_reviews"
silver_df.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

