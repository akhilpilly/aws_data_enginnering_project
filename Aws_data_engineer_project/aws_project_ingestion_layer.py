# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA5GJNPBRVG3DKGXXX")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# COMMAND ----------

emp_df = (spark.read
  .format("mysql")
  .option("host", "employeedb.cldxwxtiffdp.us-east-1.rds.amazonaws.com")
  .option("port", 33XX)
  .option("database", "emp_db")
  .option("user", "XXXX12345")
  .option("password", "XXXXXX12345")
  .option("dbtable", "emp")
  .load()
)

display(emp_df)

# COMMAND ----------

dept_df = (spark.read
  .format("mysql")
  .option("host", "employeedb.cldxwxtiffdp.us-east-1.rds.amazonaws.com")
  .option("port", 3306)
  .option("database", "emp_db")
  .option("user", "XXXX12345")
  .option("password", "XXXXXX12345")
  .option("dbtable", "dept")
  .load()
)

display(dept_df)

# COMMAND ----------


ingestion_status = 'success'
try:
  emp_df.write.mode("overwrite").parquet("s3a://mainbucketemr/raw_data/emp/")
  dept_df.write.mode("overwrite").parquet("s3a://mainbucketemr/raw_data/dept/")
except Exception as e:
   ingestion_status = 'fail'
   print(e)

print("Ingestion layer run: ", ingestion_status)

# COMMAND ----------

# MAGIC  %run /Users/pillyakhil123@gmail.com/aws_project/aws_project_raw_layer
