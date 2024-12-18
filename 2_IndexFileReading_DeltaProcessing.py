# Databricks notebook source
dbutils.fs.ls('/UHN/')

# COMMAND ----------

df = spark.read.option("multiline",True).json('/UHN/2024-05-01_ADVANCED-ELECTRIC-AND-SECURITY-INC_index.json')
df.printSchema()


# COMMAND ----------

directory_listing = dbutils.fs.ls('/UHN2024/')
file_paths = [item.path for item in directory_listing]

# COMMAND ----------

def flatten_logic(df):
    complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if
                           type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        if type(complex_fields[col_name]) == StructType:
            expanded = [col(col_name + '.' + k).alias(col_name + '_' + k) for k in
                        [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        complex_fields = dict([(field.name, field.dataType) for field in df.schema.fields if
                               type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

index_col_set = set()

for file_path in file_paths:
  jsonDf = spark.read.option("multiline",True).json(file_path)
  indexDf = flatten_logic(jsonDf)
  index_col_set.update([col for col in indexDf.columns])

print(index_col_set)

# COMMAND ----------

index_col_set

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

for file_path in file_paths:
  print('Started for : '+ file_path.split('/')[-1])
  jsonDf = spark.read.option("multiline",True).json(file_path)
  indexDf = flatten_logic(jsonDf)
  
  missing_columns = index_col_set - set(indexDf.columns)

  for column in missing_columns:
    indexDf = indexDf.withColumn(column, lit(''))

  indexDf = indexDf.withColumn("index_file_name",lit(file_path.split('/')[-1])).withColumn("last_updated_on",lit(current_timestamp()))\
                          .withColumnRenamed("reporting_structure_reporting_plans_plan_name","plan_name")\
                          .withColumnRenamed("reporting_structure_reporting_plans_plan_id_type","plan_id_type")\
                          .withColumnRenamed("reporting_structure_reporting_plans_plan_id","plan_id")\
                          .withColumnRenamed("reporting_structure_reporting_plans_plan_market_type","plan_market_type")\
                          .withColumnRenamed("reporting_structure_in_network_files_location","in_network_files_location")\
                          .withColumnRenamed("reporting_structure_in_network_files_description","in_network_files_description")\
                          .withColumnRenamed("reporting_structure_allowed_amount_file_location","allowed_amount_file_location")\
                          .withColumnRenamed("reporting_structure_allowed_amount_file_description","allowed_amount_file_description")\

  indexDf = indexDf.select("index_file_name","reporting_entity_name","reporting_entity_type","plan_id","plan_id_type","plan_name","plan_market_type","in_network_files_description","in_network_files_location","allowed_amount_file_description","allowed_amount_file_location","last_updated_on")
  
  indexDf.write.format('json').mode('append').save('/UHN2024/UHN_index_delta')

  print('Completed for : '+ file_path.split('/')[-1])

# COMMAND ----------

indexDelta = spark.read.format('delta').load('/UHN/UHN_index_delta').distinct()
display(indexDelta)

# COMMAND ----------

processedMRF = indexDelta.select('index_file_name').distinct().rdd.flatMap(lambda x: x).collect()
len(processedMRF)

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.functions import *
files = dbutils.fs.ls('/UHN/')
file_rows = [Row(path=file.path, name=file.name, size=file.size, modificationTime=file.modificationTime) for file in files]
df_files = spark.createDataFrame(file_rows)
df_files = df_files.filter(~ col('name').isin(processedMRF)).where(col('name').like('%.json'))
display(df_files)

# COMMAND ----------

file_paths = df_files.select("path").distinct().rdd.flatMap(lambda x: x).collect()
file_paths

# COMMAND ----------

# dbutils.fs.rm('/UHN/UHN_index_delta',True)
