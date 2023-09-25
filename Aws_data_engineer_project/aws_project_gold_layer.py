# Databricks notebook source
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIA5GJNPBRVG3DKGSUM")
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

gold_emp_df = (
  spark.read.parquet("s3a://mainbucketemr/gold_data/emp/")
)

display(gold_emp_df)

# COMMAND ----------

gold_dept_df = (
  spark.read.parquet("s3a://mainbucketemr/gold_data/dept/")
)

display(gold_dept_df)

# COMMAND ----------

gold_highest_earning_employee_by_department_df = (
  spark.read.parquet("s3a://mainbucketemr/gold_data/emp_transformed/")
)

display(gold_highest_earning_employee_by_department_df)

# COMMAND ----------

gold_highest_earning_employee_by_department_joined_with_department_df = (
  gold_highest_earning_employee_by_department_df.join(gold_dept_df, gold_highest_earning_employee_by_department_df.empdeptno == gold_dept_df.deptno, "inner")
  .select(
    col("empname").alias("employee_name"),
    col("emptitle").alias("employee_title"),
    col("empsalary").alias("employee_salary"),
    col("deptname").alias("department_name")
    )
)

# COMMAND ----------

display(gold_highest_earning_employee_by_department_joined_with_department_df)

# COMMAND ----------


gold_status = 'success'
try:
  gold_highest_earning_employee_by_department_joined_with_department_df.write.mode("overwrite").csv("s3a://mainbucketemr/reporting_data/emp_transfromed_joined_dept_output/", header=True)
except Exception as e:
   gold_status = 'fail'
   print(e)

print("Gold layer run: ", gold_status)

# COMMAND ----------

