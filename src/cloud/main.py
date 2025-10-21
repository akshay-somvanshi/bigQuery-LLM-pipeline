from google.cloud import bigquery
from google import genai
from google.genai.types import HttpOptions

# Connect to the BigQuery client
client_bq = bigquery.Client(project='dash-beta-e61d0')

# Retrieve the database containing relevant information
project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
table = "document"

# Get the documentID (for identifier) and raw data from BigQuery for missing parsed_data
query = f"""
    SELECT document_id, raw_data 
    FROM {project_id}.{dataset_id}.{table} 
    WHERE parsed_data IS NULL"""

query_result = client_bq.query(query).result()

# Connect to the Gemini API in VertexAI
client_gemini = genai.Client(http_options=HttpOptions(api_version="v1"))

for row in query_result:
    document_id = row.document_id 
    raw_data = row.raw_data

    # Use the raw data to generate input prompt to the model
    prompt = f"""
            You are a data extraction assistant.
            Given this JSON document data from Document AI, extract key fields:
            - total cost
            - billing period start
            - billing period end
            - consumption kWh
            Return a JSON object with these keys and standardized formats.

            Input JSON:
            {raw_data}
        """
    
    response = client_gemini.models.generate_content(
        model="gemini-2.5-flash-lite",
        contents=prompt
    )

    # Update the row for the same documentID
    update_query = f"""
        UPDATE {project_id}.{dataset_id}.{table} 
        SET parsed_data= @parsed_data
        WHERE document_id= @document_id"""
    
    update_job = client_bq.query(
        update_query, 
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("parsed_data", "JSON", response.text),
                bigquery.ScalarQueryParameter("document_id", "STRING", document_id)
            ]
        ),
    )

# Close connection to client to avoid logging errors
client_gemini.close()