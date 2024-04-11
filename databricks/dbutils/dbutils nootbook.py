# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

firstname="krishna"
lastname="bhandari"
dbutils.notebook.exit(firstname)
print(lastname)

# COMMAND ----------

print(firstname,lastname,sep=" ")

# COMMAND ----------

var1=dbutils.notebook.run('/Users/krishna.bhandari@diggibyte.com/dbutils/dbutils_data',60)
print(var1)

# Databricks notebook source
# MAGIC %run /Users/krishna.bhandari@diggibyte.com/dbutils/dbutils_widgets
