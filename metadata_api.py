import requests
import pandas as pd
import json, os
import re
from typing import Dict, List, Optional, Any, Union

env_id = os.environ.get("DBT_METADATA_ENVIRONMENT_ID")

class DbtMetadataApiClient:
    """Client for interacting with the dbt Cloud Metadata API"""
    
    def __init__(self, api_token: Optional[str] = None):
        """
        Initialize the dbt Metadata API client
        
        Args:
            api_token: Optional API token for authentication
        """
        self.base_url = "https://metadata.cloud.getdbt.com/beta/graphql"
        self.headers = {
            "Content-Type": "application/json"
        }
        if api_token:
            self.headers["Authorization"] = f"Bearer {api_token}"
    
    def execute_query(self, query: str, variables: Optional[Dict] = None) -> Dict:
        """
        Execute a GraphQL query against the dbt Metadata API
        
        Args:
            query: GraphQL query string
            variables: Optional variables for the query
            
        Returns:
            Dict containing the response data
        """

        env = {"environmentId": f"{env_id}"}

        if variables:
            variables = variables | env
        else:
            variables = env
        
        for key, value in variables.items():
            # Replace $variable with the JSON string representation (with quotes if string)
            value_str = json.dumps(value)
            query = query.replace(f"${key}", value_str)

        response = requests.post(self.base_url, json={"query": query}, headers=self.headers)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Query failed with status code {response.status_code}")
            print(response.text)
            return {}
    
    def query_model_lineage(self) -> Dict:
        """
        Query model lineage data for the specified environment
        
        Args:
            environment_id: dbt Cloud environment ID
            
        Returns:
            Dict containing model lineage data
        """
        query =  """
            query Environment {
                environment(id: $environmentId) {
                    adapterType
                    dbtProjectName
                    definition {
                        lineage(filter: { types: Model }) {
                            access
                            alias
                            database
                            filePath
                            group
                            matchesMethod
                            materializationType
                            name
                            parentIds
                            projectId
                            publicParentIds
                            resourceType
                            schema
                            tags
                            uniqueId
                            version
                        }
                    }
                }
            }
            """
        
        return self.execute_query(query)
    
    def query_column_lineage(self, node_unique_id: str,) -> Dict:
        """
        Query column lineage data for a specific column
        
        Args:
            node_unique_id: Unique ID of the dbt model
            column_name: Name of the column
            
        Returns:
            Dict containing column lineage data
        """
        query = """
            query Column {
                column(environmentId: $environmentId) {
                    lineage(nodeUniqueId: $nodeUniqueId) {
                        accountId
                        childColumns
                        depth
                        description
                        descriptionOriginColumnName
                        descriptionOriginResourceUniqueId
                        environmentId
                        error
                        errorCategory
                        isError
                        isPrimaryKey
                        name
                        nodeUniqueId
                        parentColumns
                        projectId
                        relationship
                        runId
                        transformationType
                        uniqueId
                    }
                }
            }
            """
        
        variables = {
            "nodeUniqueId": node_unique_id
        }
        
        return self.execute_query(query, variables)
    
    def extract_parent_model_name(self, parent_id: str) -> Optional[str]:
        """
        Extract the model name from parent_id if it's a model
        
        Args:
            parent_id: Parent ID string from dbt
            
        Returns:
            Formatted model name or None if not a model
        """
        if parent_id.startswith('model.'):
            match = re.match(r'model\.([^.]+)\.([^.]+)', parent_id)
            if match:
                package, model_name = match.groups()
                return f"{package}.{model_name}"
        return None
    
    def parse_model_lineage_to_df(self, response_data: Dict) -> pd.DataFrame:
        """
        Parse model lineage response into a DataFrame
        
        Args:
            response_data: API response containing model lineage
            
        Returns:
            DataFrame with parsed model lineage
        """
        if not response_data or "data" not in response_data or "environment" not in response_data["data"]:
            print("No valid data found in the response")
            return pd.DataFrame()
        
        lineage_data = response_data["data"]["environment"]["definition"]["lineage"]
        rows = []
        
        for model in lineage_data:
            row_data = {
                "project_id": model.get("projectId"),

                "database": model.get("database"),
                "schema": model.get("schema"),
                "name": model.get("name"),
                "node_id": model.get("uniqueId"),

                "tags": model.get("tags"),
                "resource_type": model.get("resourceType"),
                "materialization": model.get("materializationType"),
                
                "access": model.get("access"),
                "group": model.get("group"),
                "version": model.get("version"),

                "parent_ids": model.get("parentIds"),
                "public_parent_ids": model.get("publicParentIds")
            }
            # Extract and flatten model parent IDs
            parent_ids = model.get("parentIds", [])
            model_parents = [self.extract_parent_model_name(pid) for pid in parent_ids if self.extract_parent_model_name(pid)]
            
            # Add parent columns (up to 10)
            for i in range(1, 11):
                if i <= len(model_parents):
                    row_data[f"parent_{i}"] = model_parents[i-1]
                else:
                    row_data[f"parent_{i}"] = None
            
            rows.append(row_data)
        
        return pd.DataFrame(rows)
    
    def parse_column_lineage_to_df(self, response_data: Dict) -> pd.DataFrame:
        """
        Parse column lineage response into a DataFrame
        
        Args:
            response_data: API response containing column lineage
            
        Returns:
            DataFrame with parsed column lineage
        """
        if not response_data or "data" not in response_data or "column" not in response_data["data"]:
            print("No valid column lineage data found in the response")
            return pd.DataFrame()
        
        lineage_data = response_data["data"]["column"]["lineage"]
        rows = []
        
        for item in lineage_data:
            # Base row data with the required fields
            row_data = {
                "project_id": item.get("projectId"),

                "node_id": item.get("nodeUniqueId"),
                "column_node_id": item.get("uniqueId"),

                "column_name": item.get("name").lower(),
                "description": item.get("description"),
                "is_primary_key": item.get("isPrimaryKey"),
                "transformation_type": item.get("transformationType"),
                
                "description_origin_column_name": item.get("descriptionOriginColumnName"),
                "description_origin_resource_unique_id": item.get("descriptionOriginResourceUniqueId"),

                "relationship": item.get("relationship"),
                "parent_columns": item.get("parentColumns"),
                "child_columns": item.get("ChildColumns")
            }

            # Handle parent columns (up to 10)
            parent_columns = item.get("parentColumns", [])
            for i in range(1, 11):
                if i <= len(parent_columns):
                    row_data[f"parent_column_{i}"] = parent_columns[i-1]
                else:
                    row_data[f"parent_column_{i}"] = None
            
            rows.append(row_data)
        
        return pd.DataFrame(rows)
    
    def build_comprehensive_column_lineage(self) -> pd.DataFrame:
        """
        Build a comprehensive DataFrame of column lineages for all models in an environment
        
        Args:
            environment_id: dbt Cloud environment ID
            
        Returns:
            DataFrame with full column lineage information
        """
        # First, get all models
        model_response = self.query_model_lineage()
        model_df = self.parse_model_lineage_to_df(model_response)
        
        if model_df.empty:
            print("No models found in the environment")
            return pd.DataFrame()
        
        all_column_lineage_rows = []

        # For each model,  
        # Note: In a real implementation, you would need to know the column names
        # This is a simplified example that shows the approach
        for _, model in model_df.iterrows():
            unique_id = model["node_id"]
            if not unique_id:
                continue

            try:
                column_response = self.query_column_lineage(unique_id)
                column_df = self.parse_column_lineage_to_df(column_response)
                
                if not column_df.empty:
                    all_column_lineage_rows.append(column_df)

            except Exception as e:
                print(f"Error processing model: {unique_id} : {e}")
        
        if all_column_lineage_rows:
            return pd.concat(all_column_lineage_rows, ignore_index=True)
        else:
            return pd.DataFrame()