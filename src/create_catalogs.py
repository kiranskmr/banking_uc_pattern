# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Creation
# MAGIC 
# MAGIC This notebook provides functionality to create catalogs in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Catalog Diagram](../images/catalog_design.png)

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
# MAGIC ## Sample Catalog Creation for Risk Domains

# COMMAND ----------

risk_domains = [
    {"name": "credit_risk_dev", "comment": "Catalog for credit risk team dev environment"},
    {"name": "credit_risk_test", "comment": "Catalog for credit risk team test environment"},
    {"name": "credit_risk_staging", "comment": "Catalog for credit risk team staging environment"},
    {"name": "credit_risk_prod", "comment": "Catalog for credit risk team prod environment"},
    {"name": "liquidity_risk_dev", "comment": "Catalog for liquidity risk team dev environment"},
    {"name": "liquidity_risk_test", "comment": "Catalog for liquidity risk team test environment"},
    {"name": "liquidity_risk_staging", "comment": "Catalog for liquidity risk team staging environment"},
    {"name": "liquidity_risk_prod", "comment": "Catalog for liquidity risk team prod environment"},
    {"name": "price_risk_dev", "comment": "Catalog for price risk team dev environment"},
    {"name": "price_risk_test", "comment": "Catalog for price risk team test environment"},
    {"name": "price_risk_staging", "comment": "Catalog for price risk team staging environment"},
    {"name": "price_risk_prod", "comment": "Catalog for price risk team prod environment"},
    {"name": "compliance_risk_dev", "comment": "Catalog for compliance risk team dev environment"},
    {"name": "compliance_risk_test", "comment": "Catalog for compliance risk team test environment"},
    {"name": "compliance_risk_staging", "comment": "Catalog for compliance risk team staging environment"},
    {"name": "compliance_risk_prod", "comment": "Catalog for compliance risk team prod environment"}
]

for domain in risk_domains:
    create_catalog(domain["name"], domain["comment"])

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