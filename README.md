# Purview Demo - Purview to Unity Catalog

This is a simple demo showing how business metadata from Purview can be synchronized to Databricks.   

Requirements:   

* Purview must be configured to scan an existing Databricks workspace
* The script currently runs in a local environment and assumes you have the az cli installed and have already authenticated using az login
* A Databricks access token is required to authenticate to the Databricks workspace.  This can either be a PAT, or one generated using OAUTH
* The script assumes a table asset is being extracted from Purview.

Feel free to search for other examples that showcase the Purview and UC integration.    
This is another example that can be referenced:  [Medium Article](https://medium.com/@davegeyer/a-practical-guide-to-programmatic-data-management-using-azure-databricks-unity-catalog-and-79452911f2f5)

To use, change the following lines of code:
```
# Purview details
purview_account_name = "<your purview account>"
purview_endpoint = f"https://{purview_account_name}.catalog.purview.azure.com"

# Databricks details
metastore_id= "<your databricks metastore account>"
access_token = "<your access token>"
databricks_workspace = "<your databricks workspace URL>"
warehouse_id = "<your SQL Warehouse ID>"

# Table lookup
type_name = "databricks_table"  # this is a Purview type, assuming we are extract tables registered in Purview
catalog = "<your catalog>"
schema_name = "<your schema>"
table_name = "<your table>"
```
