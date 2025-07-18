from metadata_api import DbtMetadataApiClient
import os

def main():
    # Initialize the client
    api_token = os.environ.get("DBT_METADATA_API_TOKEN")
    client = DbtMetadataApiClient(api_token)
    
    # Query model lineage
    model_response = client.query_model_lineage()
    model_df = client.parse_model_lineage_to_df(model_response)
    
    if not model_df.empty:
        print("Model lineage data:")
        print(model_df.head())
        model_df.to_parquet("dbt_model_lineage.parquet", index=False)
        print("Model lineage saved to dbt_model_lineage.parquet")
    
    # Build comprehensive column lineage
    # Note: This is where the function that loops through all uniqueIds is called
    column_lineage_df = client.build_comprehensive_column_lineage()
    
    if not column_lineage_df.empty:
        print("\nColumn lineage data:")
        print(column_lineage_df.head())
        column_lineage_df.to_parquet("dbt_column_lineage.parquet", index=False)
        print("Column lineage saved to dbt_column_lineage.parquet")
    else:
        print("\nNo column lineage data found")

if __name__ == "__main__":
    main()