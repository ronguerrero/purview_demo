import re
import requests


pat = ""
databricks_workspace = "https://adb-4304977377728755.15.azuredatabricks.net/"
query_execution_endpoint = f"{databricks_workspace}/api/2.0/sql/statements"


class ColAttributes:
    def __init__ (self, name, description):
        self.name = name
        self.description = description
        self.classification = []

    def addClassification(self, classification):
        self.classification.append(classification)

    def str(self):       
        return f"Column Description: {self.name}\n Classifications: {self.classification}\n"
    
class UCTable:
    def __init__(self, catalog, schema, name):
        self.catalog = catalog
        self.schema = schema
        self.name = name
        self.columnList = []

    def addCol(self,col):
        self.columnList.append(col)

    def __str__(self):
        output_str = f"Table: {self.name}\n"  
        for col in self.columnList:
            output_str+=col.str()
        return output_str
    

TAG_RE = re.compile(r'<[^>]+>')
def remove_html_tags(text):
    return TAG_RE.sub('', text)


def applyToUC(ucTable):

    headers = {
        "Authorization": f"Bearer {pat}",
        "Content-Type": "application/json"
    }

    for col in ucTable.columnList:
        column = col.name
        # purview encodes the description as HTML, strip it for now
        comment = remove_html_tags(col.description)

        payload = {
            "warehouse_id": "c0bdb640d3761468",
            "catalog" : ucTable.catalog,
            "schema": ucTable.schema,
            "statement": f"ALTER TABLE {ucTable.name} ALTER COLUMN {column} COMMENT '{comment}';",
            "parameters": [
                { "name": "comment", "value": f"{comment}", "type": "STRING" }
            ]
        }
            
        response = requests.post(query_execution_endpoint, headers=headers, json=payload)
        # Check response
        if response.status_code == 200:
            print("Request successful!")
            print("Response:", response.json())
        else:
            print(f"Request failed with status code {response.status_code}")
            print("Error:", response.text)


        tag_clause = ""
        if len(col.classification) > 0:
            tags = ""
            for tag in col.classification:
                if len(tags) > 0:
                    tags+=","
                tags += tag

            payload = {
                "warehouse_id": "c0bdb640d3761468",
                "catalog" : ucTable.catalog,
                "schema": ucTable.schema,
                "statement": f"ALTER TABLE customer ALTER COLUMN {column} SET TAGS ( 'classifications' ='{tags}' );",
            }

            response = requests.post(query_execution_endpoint, headers=headers, json=payload)
            # Check response
            if response.status_code == 200:
                print("Request successful!")
                print("Response:", response.json())
            else:
                print(f"Request failed with status code {response.status_code}")
                print("Error:", response.text)



        
