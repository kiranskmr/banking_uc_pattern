# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Storage Credential Registration
# MAGIC 
# MAGIC This notebook provides functionality to register storage credentials in Unity Catalog.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    AzureManagedIdentity,
    AzureServicePrincipal,
    AwsIamRole
)

# COMMAND ----------

def check_storage_credential_exists(workspace_client, credential_name):
    """
    Check if a storage credential with the given name already exists.
    
    Args:
        workspace_client: The Databricks workspace client
        credential_name: Name of the storage credential to check
    
    Returns:
        bool: True if the storage credential exists, False otherwise
    """
    try:
        workspace_client.storage_credentials.get(credential_name)
        return True
    except Exception:
        return False

# COMMAND ----------

def register_storage_credential(credential_name, credential_type, credential_details, comment=None):
    workspace_client = WorkspaceClient()
    
    if check_storage_credential_exists(workspace_client, credential_name):
        print(f"Storage credential '{credential_name}' already exists. Skipping creation.")
        return workspace_client.storage_credentials.get(credential_name)
    
    print(f"\nRegistering storage credential '{credential_name}'...")
    
    try:
        # Create the appropriate credential object based on type
        if credential_type == "azure_managed_identity":
            credential = AzureManagedIdentity(
                managed_identity_id=credential_details["managed_identity_id"]
            )
        elif credential_type == "azure_service_principal":
            credential = AzureServicePrincipal(
                directory_id=credential_details["directory_id"],
                application_id=credential_details["application_id"],
                client_secret=credential_details["client_secret"]
            )
        elif credential_type == "aws_iam_role":
            credential = AwsIamRole(
                role_arn=credential_details["role_arn"]
            )
        else:
            raise ValueError(f"Unsupported credential type: {credential_type}")
        
        # Create the storage credential
        storage_credential = workspace_client.storage_credentials.create(
            name=credential_name,
            comment=comment,
            **{credential_type: credential}
        )
        
        print(f"Successfully registered storage credential: {storage_credential.name}")
        if comment:
            print(f"Comment: {storage_credential.comment}")
        return storage_credential
    except Exception as e:
        print(f"Error registering storage credential: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interactive Storage Credential Registration

# COMMAND ----------

print("Unity Catalog Storage Credential Registration")
print("===========================================")

credential_name = input("Enter the storage credential name: ")
print("\nCredential Types:")
print("1. Azure Managed Identity")
print("2. Azure Service Principal")
print("3. AWS IAM Role")
credential_type_choice = input("Enter the credential type (1-3): ")

credential_details = {}
if credential_type_choice == "1":
    credential_type = "azure_managed_identity"
    credential_details["managed_identity_id"] = input("Enter the managed identity ID: ")
elif credential_type_choice == "2":
    credential_type = "azure_service_principal"
    credential_details["directory_id"] = input("Enter the directory ID: ")
    credential_details["application_id"] = input("Enter the application ID: ")
    credential_details["client_secret"] = input("Enter the client secret: ")
elif credential_type_choice == "3":
    credential_type = "aws_iam_role"
    credential_details["role_arn"] = input("Enter the IAM role ARN: ")
else:
    raise ValueError("Invalid credential type choice")

comment = input("\nEnter a comment for the storage credential (optional): ")

storage_credential = register_storage_credential(
    credential_name=credential_name,
    credential_type=credential_type,
    credential_details=credential_details,
    comment=comment if comment else None
)

print(f"\nStorage credential registered: {storage_credential.name}") 