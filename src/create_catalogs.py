# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog - Catalog Creation
# MAGIC 
# MAGIC This notebook provides functionality to create catalogs in Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo

# COMMAND ----------

def check_catalog_exists(workspace_client, catalog_name):
    try:
        workspace_client.catalogs.get(catalog_name)
        return True
    except Exception:
        return False

# COMMAND ----------

def create_catalog(catalog_name, catalog_comment=None):
    workspace_client = WorkspaceClient()
    
    if check_catalog_exists(workspace_client, catalog_name):
        print(f"Catalog '{catalog_name}' already exists. Skipping creation.")
        return workspace_client.catalogs.get(catalog_name)
    
    print(f"\nCreating catalog '{catalog_name}'...")
    
    try:
        catalog = workspace_client.catalogs.create(
            name=catalog_name,
            comment=catalog_comment
        )
        print(f"Successfully created catalog: {catalog.name}")
        if catalog_comment:
            print(f"Comment: {catalog.comment}")
        return catalog
    except Exception as e:
        print(f"Error creating catalog: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Catalog Creation

# COMMAND ----------

catalog_name = input("Enter the catalog name: ")
catalog_comment = input("Enter a comment for the catalog (optional): ")

catalog = create_catalog(
    catalog_name=catalog_name,
    catalog_comment=catalog_comment if catalog_comment else None
)

print(f"\nCatalog created: {catalog.name}") 