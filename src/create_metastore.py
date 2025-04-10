# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Metastore Creation
# MAGIC 
# MAGIC This notebook provides functionality to create metastores in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Metastore Diagram](../images/metastore_deployment.png)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MetastoreInfo

# COMMAND ----------

def check_metastore_exists(workspace_client, metastore_name):
    try:
        workspace_client.metastores.get(metastore_name)
        return True
    except Exception:
        return False

# COMMAND ----------

def create_metastore(metastore_name, region, comment=None):
    workspace_client = WorkspaceClient()
    
    if check_metastore_exists(workspace_client, metastore_name):
        print(f"Metastore '{metastore_name}' already exists. Skipping creation.")
        return workspace_client.metastores.get(metastore_name)
    
    print(f"\nCreating metastore '{metastore_name}' in region '{region}'...")
    
    try:
        metastore = workspace_client.metastores.create(
            name=metastore_name,
            region=region,
            comment=comment
        )
        print(f"Successfully created metastore: {metastore.name}")
        print(f"Region: {metastore.region}")
        if comment:
            print(f"Comment: {metastore.comment}")
        return metastore
    except Exception as e:
        print(f"Error creating metastore: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Metastore Creation

# COMMAND ----------

metastore_name = input("Enter the metastore name: ")
region = input("Enter the Azure region (e.g., 'uk-south', 'france-central'): ")
comment = input("Enter a comment for the metastore (optional): ")

metastore = create_metastore(
    metastore_name=metastore_name,
    region=region,
    comment=comment if comment else None
)

print(f"\nMetastore created: {metastore.name}")
print(f"Region: {metastore.region}") 