# Databricks notebook source
spark.conf.set("STORAGE ACCOUNT",
    "STORAGE ACCOUNT KEY")

# COMMAND ----------

gold_path = "abfss://ecommerce@cdacprojects.dfs.core.windows.net/silver_layer_reviews"

gold_df = spark.read.format("delta").option("header", True).option("inferSchema", True).load(gold_path)

# COMMAND ----------

display(gold_df)  # Ensure 'gold_df' is loaded as a Delta table, not as a Parquet table.

# COMMAND ----------

# MAGIC %pip install nltk

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from nltk.sentiment.vader import SentimentIntensityAnalyzer
import nltk
nltk.download('vader_lexicon')

from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType


# COMMAND ----------

# Initialize VADER
sia = SentimentIntensityAnalyzer()


# COMMAND ----------

# UDF to get compound sentiment score
def get_sentiment_score(text):
    if text and text.strip() != "":
        return float(sia.polarity_scores(text)["compound"])
    else:
        return 0.0

get_sentiment_score_udf = udf(get_sentiment_score, FloatType())


# COMMAND ----------


# Apply UDF on cleaned reviews
gold_df = gold_df.withColumn("sentiment_score", get_sentiment_score_udf(col("review_comment_clean")))


# COMMAND ----------

# UDF for sentiment category
def get_sentiment_label(score):
    if score >= 0.05:
        return "positive"
    elif score <= -0.05:
        return "negative"
    else:
        return "neutral"

get_sentiment_label_udf = udf(get_sentiment_label, StringType())


# COMMAND ----------


gold_df = gold_df.withColumn("sentiment_label", get_sentiment_label_udf(col("sentiment_score")))



# COMMAND ----------

gold_path = "abfss://ecommerce@cdacprojects.dfs.core.windows.net/gold_layer_reviews"
gold_df.write.format("delta").mode("overwrite").save(gold_path)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

display(gold_df)

# COMMAND ----------

gold_df.createOrReplaceTempView("gold_tb")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT sentiment_label, COUNT(*) AS review_count
# MAGIC FROM gold_tb
# MAGIC GROUP BY sentiment_label;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT review_star_rating, ROUND(AVG(sentiment_score), 3) AS avg_sentiment
# MAGIC FROM gold_tb
# MAGIC GROUP BY review_star_rating
# MAGIC ORDER BY review_star_rating;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DATE_TRUNC('month', review_date) AS month,
# MAGIC        sentiment_label,
# MAGIC        COUNT(*) AS review_count
# MAGIC FROM gold_tb
# MAGIC GROUP BY month, sentiment_label
# MAGIC ORDER BY month ASC;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT reviewed_product_asin,
# MAGIC        COUNT(*) AS total_reviews,
# MAGIC        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_reviews,
# MAGIC        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_reviews
# MAGIC FROM gold_tb
# MAGIC GROUP BY reviewed_product_asin
# MAGIC ORDER BY total_reviews DESC;
# MAGIC

# COMMAND ----------

