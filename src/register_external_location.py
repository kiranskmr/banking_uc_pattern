# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog - External Location Registration
# MAGIC 
# MAGIC This notebook provides functionality to register external locations in Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ExternalLocationInfo

# COMMAND ----------

def check_external_location_exists(workspace_client, location_name):
    """
    Check if an external location with the given name already exists.
    
    Args:
        workspace_client: The Databricks workspace client
        location_name: Name of the external location to check
    
    Returns:
        bool: True if the external location exists, False otherwise
    """
    try:
        workspace_client.external_locations.get(location_name)
        return True
    except Exception:
        return False

# COMMAND ----------

def register_external_location(
    location_name,
    location_comment,
    storage_url,
    credential_name
):
    """
    Register an external location in Unity Catalog.
    
    Args:
        location_name: Name of the external location
        location_comment: Comment for the external location
        storage_url: URL of the storage location (e.g., abfss://container@storageaccount.dfs.core.windows.net/path)
        credential_name: Name of the storage credential to use
    
    Returns:
        ExternalLocationInfo: The created or existing external location
    """
    # Initialize the Databricks workspace client
    workspace_client = WorkspaceClient()
    
    # Check if external location already exists
    if check_external_location_exists(workspace_client, location_name):
        print(f"External location '{location_name}' already exists. Skipping creation.")
        return workspace_client.external_locations.get(location_name)
    
    # Create the external location
    print(f"\nRegistering external location '{location_name}'...")
    
    try:
        # Create the external location
        external_location = workspace_client.external_locations.create(
            name=location_name,
            comment=location_comment,
            url=storage_url,
            credential_name=credential_name
        )
        print(f"Successfully registered external location: {external_location.name}")
        print(f"Location URL: {external_location.url}")
        if location_comment:
            print(f"Comment: {external_location.comment}")
        return external_location
    except Exception as e:
        print(f"Error registering external location: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive External Location Registration

# COMMAND ----------

print("Unity Catalog External Location Registration")
print("===========================================")

# Get location details
location_name = input("Enter the external location name: ")
location_comment = input("Enter a comment for the external location: ")

# Get storage URL
print("\nStorage URL Examples:")
print("- Azure: abfss://container@storageaccount.dfs.core.windows.net/path")
print("- AWS: s3://bucket-name/path")
storage_url = input("Enter the storage URL: ")

# Get credential name
credential_name = input("Enter the name of the storage credential to use: ")

# Register the external location
external_location = register_external_location(
    location_name=location_name,
    location_comment=location_comment,
    storage_url=storage_url,
    credential_name=credential_name
)

print(f"\nExternal location registered: {external_location.name}")
print(f"URL: {external_location.url}") 