# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA5GJNPBRVG3DKGSUM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# COMMAND ----------

from pyspark.sql.functions import dense_rank, desc
from pyspark.sql.window import Window

# COMMAND ----------



# COMMAND ----------

silver_emp_df = (
  spark.read.parquet("s3a://mainbucketemr/silver_data/emp/")
)

display(silver_emp_df)

# COMMAND ----------

silver_dept_df = (
  spark.read.parquet("s3a://mainbucketemr/silver_data/dept/")
)

display(silver_dept_df)

# COMMAND ----------

silver_highest_earning_employee_by_department_df = (
  silver_emp_df
  .withColumn("drnk", dense_rank().over(Window.partitionBy("empdeptno").orderBy(desc("empsalary"))))
  .filter("drnk = 1")
  .select("empname", "empdeptno", "emptitle", "empsalary")
)

# COMMAND ----------


silver_status = 'success'
try:
  silver_emp_df.write.mode("overwrite").parquet("s3a://mainbucketemr/gold_data/emp/")
  silver_dept_df.write.mode("overwrite").parquet("s3a://mainbucketemr/gold_data/dept/")
  silver_highest_earning_employee_by_department_df.write.mode("overwrite").parquet("s3a://mainbucketemr/gold_data/emp_transformed/")
except Exception as e:
   silver_status = 'fail'
   print(e)

print("Silver layer run: ", silver_status)

# COMMAND ----------

# MAGIC %run /Users/pillyakhil123@gmail.com/aws_project/aws_project_gold_layer