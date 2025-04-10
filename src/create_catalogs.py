# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Creation
# MAGIC 
# MAGIC This notebook provides functionality to create catalogs in Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo

