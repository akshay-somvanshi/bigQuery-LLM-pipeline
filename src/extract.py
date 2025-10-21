from google.cloud import bigquery

# Connect to the BigQuery client
client_bq = bigquery.Client(project='dash-beta-e61d0')

# Retrieve the database containing relevant information
dataset_id = 'dash-beta-e61d0.dash_beta_database'
# The table containing the raw data 
table = "document"

query = f"SELECT raw_data FROM {dataset_id}.{table} WHERE parsed_data IS NULL"
query_job = client_bq.query(query)
# Convert iterator to list (faster than list(iterator))
sql_result = [*query_job.result()];
