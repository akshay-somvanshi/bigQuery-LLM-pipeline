from google.cloud import bigquery

# Retrieve the database containing relevant information
project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
# The table containing the raw data 
table = "document"

# Connect to the BigQuery client
client_bq = bigquery.Client(project_id)

# Get the documentID (for identifier) and raw data from BigQuery for missing parsed_data
query = f"""
    SELECT document_id, raw_data 
    FROM `{project_id}.{dataset_id}.{table}` 
    WHERE parsed_data IS NULL"""

query_result = client_bq.query(query).result()

# Debug- convert result to list (faster than list(query_result))
# result_list = [*query_result]
# print(result_list)
