from transform import response
from google.cloud import bigquery

client_bq = bigquery.Client()

# Update the table 
project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
table = 'document'

# Update the row for the same documentID
update_query = f"""
    UPDATE `{project_id}.{dataset_id}.{table}`
    SET parsed_data= @parsed_data
    WHERE document_id= @document_id
    RETURNING *"""

update_job = client_bq.query(
    update_query, 
    job_config=bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("parsed_data", "STRING", response.text),
            bigquery.ScalarQueryParameter("document_id", "STRING", document_id)
        ]
    ),
).result()