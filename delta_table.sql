-- Databricks notebook source
CREATE TABLE employee
(id int, name varchar(20), salary double)

-- COMMAND ----------

insert into employee values (1,"Amit",200.90),(2,"Abhika",308)

-- COMMAND ----------

select * from employee

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

update employee
set salary = salary +100
where name like "A%"

-- COMMAND ----------

describe detail employee

-- COMMAND ----------

select * from employee

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/employee

-- COMMAND ----------

describe history employee

-- COMMAND ----------

-- MAGIC %fs ls 'dbfs:/user/hive/warehouse/employee/_delta_log'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. `00000000000000000002.crc`: Contains cyclic redundancy check information for data integrity checks in Delta Lake, ensuring file integrity.

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000002.crc

-- COMMAND ----------

-- MAGIC %md
-- MAGIC `00000000000000000002.json`: JSON file storing metadata and transaction logs for the Delta Lake table 'employee', crucial for maintaining ACID properties.

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/employee/_delta_log/00000000000000000002.json
