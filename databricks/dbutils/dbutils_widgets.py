# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.combobox(name='fruitsCD',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits ComboBox')

# COMMAND ----------

dbutils.widgets.dropdown(name='fruitsDD',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits Dropdown')

# COMMAND ----------

dbutils.widgets.multiselect(name='fruitsMS',defaultValue='apple',choices=['apple','banana','orange'],label='Fruits Multiselect')

# COMMAND ----------

dbutils.widgets.text(name='fruitTB',defaultValue='apple',label='Fruit Textbox')

# COMMAND ----------

dbutils.widgets.get('fruitsCD')

# COMMAND ----------

dbutils.widgets.getArgument('fruitsMS1','error:this widget is not available.')

# COMMAND ----------

dbutils.widgets.remove('fruitsMS')

# COMMAND ----------

dbutils.widgets.removeAll()
