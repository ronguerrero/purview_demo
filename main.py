import requests
from azure.identity import DefaultAzureCredential
from uctable import *

# Authentication
credential = DefaultAzureCredential()
access_token = credential.get_token("https://purview.azure.net/.default").token

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

# purview qualified names must be referred to as follows:
qualified_name = f"databricks://{metastore_id}/catalogs/{catalog}/schemas/{schema_name}/tables/{table_name}"  # Replace with the qualified name of your entity


# purview entity endpoint
entity_lookup_url = f"{purview_endpoint}/api/atlas/v2/entity/uniqueAttribute/type/{type_name}"

# Send the request with parameters in the payload
params = {
    "attr:qualifiedName": qualified_name
}

# Set headers with the obtained access token
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# unpack the reponse
response = requests.get(entity_lookup_url, params=params, headers=headers)
# Check the response
if response.status_code == 200:
    print("Request successful!")
    print("Response:", response.json())

    entity = response.json()

    # referredEntities contains the column definitions
    referredEntities = entity['referredEntities']

    table = UCTable(catalog, schema_name, table_name)
    for item in referredEntities:

        # extract common column attributes - description
        name = entity['referredEntities'][item]['attributes']['name']
        description = entity['referredEntities'][item]['attributes']['userDescription']
        col = ColAttributes(name, description)

        # extract classifications -> will  be tagged in UC
        for classification in entity['referredEntities'][item]['classifications']:
           classificationName = classification['typeName']
           col.addClassification(classification['typeName'])

        # assemble UC Table 
        table.addCol(col)
    print(table)

else:
    print(f"Request failed with status code {response.status_code}")
    print("Error:", response.text)


databricksEnv = DatabricksEnv(databricks_workspace, warehouse_id, pat)

# do the databricks UC stuff
applyToUC(databricksEnv, table) 
#clearUCTable(databricksEnv, table)