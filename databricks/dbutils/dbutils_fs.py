# Databricks notebook source
dbutils.fs.help()

# COMMAND ----------

dbutils.fs.cp('/FileStore/resource/inventory_records.csv','/FileStore/my_data/inventory_records.csv')

# COMMAND ----------

dbutils.fs.head('/FileStore/resource/inventory_records.csv',200)

# COMMAND ----------

dbutils.fs.mkdirs('/FileStore/new_folder')

# COMMAND ----------

dbutils.fs.rm('/FileStore/new_folder')

# COMMAND ----------

display(dbutils.fs.ls('/FileStore'))

# COMMAND ----------

dbutils.fs.head('/FileStore/resource/file.txt')

# COMMAND ----------

dbutils.fs.put('/FileStore/resource/file.txt','Hello, I am Krishnna, '
               'I just overwrite this file',True)

# COMMAND ----------

dbutils.fs.head('/FileStore/resource/file.txt')

# COMMAND ----------

dbutils.fs.mount(
    source='wasbs://<containerName>@<accountName>.blob.core.windows.net/',
    mount_point='/mnt/<mountName>',
    extra_configs={'fs.azure.account.key.blobstoragecshaik.blob.core.windows.net':'accountkey'}
)

# COMMAND ----------

dbutils.fs.ls('/mnt/<mountName>')

# COMMAND ----------

dbutils.fs.unmount("/mnt/<mountName>")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------


