# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA5GJNPBRVG3DKGSUM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window

# COMMAND ----------

raw_emp_df = (
  spark.read.parquet("s3a://mainbucketemr/raw_data/emp/")
)

display(raw_emp_df)

# COMMAND ----------

raw_dept_df = (
  spark.read.parquet("s3a://mainbucketemr/raw_data/dept/")
)

display(raw_dept_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Data quality checks:

# COMMAND ----------

# DBTITLE 1,Null count check for each column
display(raw_emp_df.select([count(when(isnan(c), c)).alias(c) for c in raw_emp_df.columns]))

# COMMAND ----------

# DBTITLE 1,Null count check for each column
display(raw_dept_df.select([count(when(isnan(c), c)).alias(c) for c in raw_dept_df.columns]))

# COMMAND ----------

# DBTITLE 1,Duplicate records check
display(
  raw_emp_df.withColumn("rn", row_number().over(Window.partitionBy("empid").orderBy("empid")))
  .filter("rn > 1").drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Unique records check
display(
  raw_emp_df.withColumn("rn", row_number().over(Window.partitionBy("empid").orderBy("empid")))
  .filter("rn = 1").drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Duplicate records check
display(
  raw_dept_df.withColumn("rn", row_number().over(Window.partitionBy("deptno").orderBy("deptno")))
  .filter("rn > 1").drop("rn")
)

# COMMAND ----------

# DBTITLE 1,Unique records check
display(
  raw_dept_df.withColumn("rn", row_number().over(Window.partitionBy("deptno").orderBy("deptno")))
  .filter("rn = 1").drop("rn")
)

# COMMAND ----------


raw_status = 'success'
try:
  raw_emp_df.write.mode("overwrite").parquet("s3a://mainbucketemr/silver_data/emp/")
  raw_dept_df.write.mode("overwrite").parquet("s3a://mainbucketemr/silver_data/dept/")
except Exception as e:
   raw_status = 'fail'
   print(e)

print("Raw layer run: ", raw_status)

# COMMAND ----------

# MAGIC  %run /Users/pillyakhil123@gmail.com/aws_project/aws_project_silver_layer