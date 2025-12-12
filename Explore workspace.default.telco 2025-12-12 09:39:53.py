# Databricks notebook source
from pyspark.sql import functions as F

telco = spark.table("telco")  
telco.printSchema()
display(telco.limit(5))



# COMMAND ----------

from pyspark.sql import functions as F

telco = telco.withColumn(
    "TotalCharges_clean",
    F.when(F.trim(F.col("TotalCharges")) == "", None)
     .otherwise(F.col("TotalCharges").cast("double"))
)


# COMMAND ----------

df_customer_monthly = telco.select("customerID", "MonthlyCharges")
display(df_customer_monthly)


# COMMAND ----------

contract_counts = telco.groupBy("Contract").count()
display(contract_counts)


# COMMAND ----------

multiple_lines_count = telco.filter(F.col("MultipleLines") == "Yes").count()
multiple_lines_count


# COMMAND ----------

tenure_stats = telco.agg(
    F.min("tenure").alias("min_tenure"),
    F.max("tenure").alias("max_tenure")
).collect()[0]

min_tenure = tenure_stats["min_tenure"]
max_tenure = tenure_stats["max_tenure"]

min_tenure, max_tenure


# COMMAND ----------

monthly_by_gender = (
    telco.groupBy("gender")
         .agg(F.avg("MonthlyCharges").alias("avg_monthly_charges"))
)

display(monthly_by_gender)


# COMMAND ----------

yearly_avg = (
    telco.filter(F.col("Contract") == "One year")
         .agg(F.avg("MonthlyCharges"))
         .collect()[0][0]
)

others_avg = (
    telco.filter(F.col("Contract") != "One year")
         .agg(F.avg("MonthlyCharges"))
         .collect()[0][0]
)

yearly_pays_more = yearly_avg > others_avg
yearly_pays_more


# COMMAND ----------

telco = telco.withColumn(
    "avg_charges",
    F.when(
        (F.col("tenure") == 0) |
        F.col("tenure").isNull() |
        F.col("TotalCharges_clean").isNull(),
        None
    ).otherwise(F.col("TotalCharges_clean") / F.col("tenure"))
)

display(
    telco.select(
        "customerID",
        "tenure",
        "TotalCharges_clean",
        "avg_charges"
    ).limit(10)
)


# COMMAND ----------


