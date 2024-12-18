# Databricks notebook source
indexDelta = spark.read.format('delta').load('/UHN/UHN_index_delta').distinct()
# display(indexDelta)

# COMMAND ----------

from pyspark.sql.functions import *
indexDelta1 = indexDelta.select('index_file_name').distinct().withColumn("CustomerName",regexp_replace(split(split(col('index_file_name'),'2024-05-01_')[1],'_index.json')[0], r'^[^A-Za-z0-9]', ""))
display(indexDelta1)

# COMMAND ----------

display(indexDelta.select('plan_name').distinct())

# COMMAND ----------

keywords = [
    "HEALTH-CARE-BENEFITS-PLAN", "GROUP-BENEFIT-PLAN", "GROUP-HEALTH-BENEFIT-PLAN","GROUP-BENEFIT-HEALTH-PLAN",
    "EMPLOYEE-BENEFIT-PLANS", "CHOICE-EPO", "EPO", "CHOICE-PLUS-POS","HEALTH-CARE-FUND",
    "NATIONAL-PPO", "POS-CHOICE-PLUS", "SELECT-EPO", "SELECT-PLUS-POS",
    "UHC-CHOICE", "POS", "CHOICE"
]

# Define UDF to extract matching keyword
def extract_keyword(plan_name):
    for keyword in keywords:
        if keyword in plan_name:
            return keyword
    return None

# Register UDF
extract_keyword_udf = udf(extract_keyword, StringType())

def remove_keyword(plan_name):
    for keyword in keywords:
        if keyword in plan_name:
            return plan_name.replace(keyword, "").strip("-")  # Remove keyword and extra hyphens
    return plan_name

# Register UDF
remove_keyword_udf = udf(remove_keyword, StringType())

# Apply UDF to create new column with extracted keyword
indexDelta_withAddDf = indexDelta.withColumn("ProductName", extract_keyword_udf(col("plan_name"))) \
                             .withColumn("CustomerName",regexp_replace(split(split(col('index_file_name'),'2024-05-01_')[1],'_index.json')[0], r'^[^A-Za-z0-9]', ""))\
                             .withColumn("CustomerName", remove_keyword_udf(col("CustomerName")))

# Show the DataFrame with the new column
display(indexDelta_withAddDf)

# COMMAND ----------

snowflake_options = {
    "sfURL": "https://ivzwcgm-df12555.snowflakecomputing.com",
    "sfUser": "tkapila",
    "sfPassword": "Gaur@1041",
    "sfDatabase": "UHN",
    "sfSchema": "TIC_BI",
    "sfWarehouse": "COMPUTE_WH"
}

indexDelta_withAddDf.write \
    .format("net.snowflake.spark.snowflake") \
    .options(**snowflake_options) \
    .option("dbtable", "TIC_UHN_INDEX_INTEL") \
    .mode("overwrite") \
    .save()
