from google import genai
from google.genai.types import HttpOptions
from extract import query_result 

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

    print(response.text)

# Close connection to client to avoid logging errors
client_gemini.close()