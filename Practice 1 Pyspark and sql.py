# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame API

# COMMAND ----------

emp_df=spark.read.format("csv").option("header","true").option("inferschema","true").load("/FileStore/emplyee.csv")
emp_df.show()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

emp_df.select("name").show() #row

# COMMAND ----------

# MAGIC %md
# MAGIC OR

# COMMAND ----------

emp_df.select(col("name")).show() #column

# COMMAND ----------

emp_df.select(col("id")).show()

# COMMAND ----------

emp_df.select(col("id")+5).show()

# COMMAND ----------

emp_df.select("id","name","age").show()

# COMMAND ----------

emp_df.select(col("id"),col("name"),col("age")).show()

# COMMAND ----------

    emp_df.select(expr("id as emp_id"),expr("name as emp_name"),expr("age as emp_age"),expr("concat(name,address)")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL

# COMMAND ----------

emp_df.createOrReplaceTempView("emp_tbl")

# COMMAND ----------

spark.sql("""

select * from emp_tbl;

""").show()

# COMMAND ----------

emp_df.select("*").show()

# COMMAND ----------

spark.sql("""
select id,name ,age from emp_tbl;
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC DataFrame API

# COMMAND ----------

emp_df.select(col("id").alias("emp_id"),"name","age").show()

# COMMAND ----------

emp_df.filter((col("salary")>150000) & (col("age")<18)).show()

# COMMAND ----------

emp_df.select("*",lit("kumar").alias("last_name")).show()

# COMMAND ----------

emp_df.withColumnRenamed("id","emp_id").show()

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

emp_df.withColumn("id",col("id").cast("string")).printSchema()

# COMMAND ----------

emp_df.withColumn("id",col("id").cast("string"))\
    .withColumn("salary",col("salary").cast("long")).printSchema()

# COMMAND ----------

emp_df.drop("id",col("name")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Spark Sql
# MAGIC

# COMMAND ----------

spark.sql("""

select * from emp_tbl where salary>150000 and age <18

""").show()

# COMMAND ----------

spark.sql("""

select *,"kumar" as last_name from emp_tbl where salary>150000 and age <18

""").show()

# COMMAND ----------

spark.sql("""

select *,"kumar" as last_name, concat(name,address ) as full_detail from emp_tbl where salary>150000 and age <18

""").show()

# COMMAND ----------

spark.sql("""

select *,"kumar" as last_name, concat(name,address ) as full_detail , id as emp_id from emp_tbl where salary>150000 and age <18

""").show()

# COMMAND ----------

spark.sql("""

select *,"kumar" as last_name, concat(name,address ) as full_detail , id as emp_id, cast(emp_id as string) from emp_tbl where salary>150000 and age <18

""").printSchema()

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(14 ,'Priya',80000,  18),
(14 ,'Priya',60000,  18),
(14 ,'Priya',80000,  18),]

schema=["id","name","sal","mgr_data"]

df=spark.createDataFrame(data,schema=schema)
df.show()


# COMMAND ----------

df.count()

# COMMAND ----------

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)]

schema=["id","name","sal","mgr_data"]

df1=spark.createDataFrame(data1,schema)
df1.show()

# COMMAND ----------

df1.count()

# COMMAND ----------

data=df.union(df1)
data.show()

# COMMAND ----------

data.count()

# COMMAND ----------

data1=df.unionAll(df1)
data1.show()

# COMMAND ----------


data1.count()

# COMMAND ----------

data.createOrReplaceTempView("df_tbl")
data1.createOrReplaceTempView("df1_tbl")

# COMMAND ----------

spark.sql("""
select * from df_tbl union select * from df1_tbl
""").count()

# COMMAND ----------

spark.sql("""
select * from df_tbl union all select * from df1_tbl
""").count()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]
schema=["id","sal","mgr_data","name"]

df3=spark.createDataFrame(wrong_column_data,schema)
df3.show()

# COMMAND ----------

df1.union(df3).show()

# COMMAND ----------

df1.unionByName(df3).show()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

schema=["id","sal","mgr_data","name","bonus"]

df4=spark.createDataFrame(wrong_column_data,schema)
df4.show()

# COMMAND ----------

df4.select("id","sal","name","mgr_data").unionByName(df1).show()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]
schema=["id","sal","mgr_data","nam"]

df5=spark.createDataFrame(wrong_column_data,schema)
df5.show()

# COMMAND ----------

# df1.unionByName(df5).show()

# COMMAND ----------


