from google.cloud import bigquery
import json
from datetime import datetime

project_id = 'dash-beta-e61d0'
dataset_id = 'dash_beta_database'
user_table = 'user'
document_table = 'document'

# Connect to client
client_bq = bigquery.Client(project_id)

# Get the user information including the industry
query_user = f"""
        SELECT user_id as user_id, industry as industry 
        FROM `{project_id}.{dataset_id}.{user_table}`"""

user_info = client_bq.query(query_user).result()

for user in user_info:
    user_id = user.user_id
    # Access the industry information from the query
    industry = user.industry

    # FOR NOW - Let the industry always be manufacturing
    # match industry:
    #     case "Manufacturing":
    #         industry_table = 'industry_manufacturing'
    #     case "Banks":
    #         industry_table = 'industry_bank'

    industry_table = 'industry_manufacturing'

    # Get all the document_ids and parsed_data for the user we are processing
    query_document = f"""
        SELECT document_id as document_id, parsed_data as data
        FROM `{project_id}.{dataset_id}.{document_table}` 
        WHERE user_id = @user_id"""

    job_config = bigquery.QueryJobConfig(
        query_parameters = [
            bigquery.ScalarQueryParameter("user_id", "STRING", user_id)
        ]
    )

    doc_info = client_bq.query(query_document, job_config=job_config).result()

    for doc in doc_info:
        document_id = doc.document_id
        raw_data = doc.data
        parsed_start_date = None
        parsed_end_date = None
        
        # Remove Markdown formatting if present
        if raw_data.startswith("```"):
            # Strip the triple backticks and optional 'json' label
            raw_data = raw_data.strip("`").lstrip("json").strip()
        try:
            data = json.loads(raw_data)
            try:
                if data.get("billing_period_start"):
                    parsed_start_date = datetime.strptime(data.get("billing_period_start"), "%b,%y")
            except ValueError:
                parsed_start_date = None

            try:
                if data.get("billing_period_end"):
                    parsed_end_date = datetime.strptime(data.get("billing_period_end"), "%b,%y")
            except ValueError:
                parsed_end_date = None

        except json.JSONDecodeError as e:
            print(f"Failed to parse JSON: {e}")
            break

        query_industry = f"""
                INSERT INTO `{project_id}.{dataset_id}.{industry_table}` 
                (user_id, document_id, ele_consumption_kWh, period_start, period_end)
                VALUES (@user_id, @document_id, @ele_cons, @period_start, @period_end)"""
        
        industry_config = bigquery.QueryJobConfig(
            query_parameters= [
                bigquery.ScalarQueryParameter("user_id", "STRING", user_id),
                bigquery.ScalarQueryParameter("document_id", "STRING", document_id),
                bigquery.ScalarQueryParameter("ele_cons", "STRING", data.get("consumption_kWh")),
                bigquery.ScalarQueryParameter("period_start", "TIMESTAMP", parsed_start_date), 
                bigquery.ScalarQueryParameter("period_end", "TIMESTAMP", parsed_end_date)
            ]
        )

        result = client_bq.query(query_industry, job_config = industry_config).result()
    
