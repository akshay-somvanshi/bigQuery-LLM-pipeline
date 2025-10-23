# BigQuery LLM Pipeline

## Overview

This project implements a data processing pipeline that extracts data from a Google BigQuery table, processes it using a Large Language Model (LLM) from Google's Gemini family, and loads the processed data back into the same BigQuery table.

The primary use case is to parse unstructured or semi-structured data (e.g., JSON from Document AI) into a structured format using LLMs.

## Features

*   **Extract**: Fetches rows from a BigQuery table where processed data is missing.
*   **Transform**: Uses the Gemini LLM to parse and extract key fields from the raw data.
*   **Load**: Updates the BigQuery table with the structured data extracted by the LLM.

## Getting Started

### Prerequisites

*   Python 3.8+
*   A Google Cloud Platform (GCP) project.
*   The BigQuery and Vertex AI APIs enabled in your GCP project.
*   Authenticated `gcloud` CLI on your local machine.

### Installation

1.  **Clone the repository:**
    ```bash
    git clone <your-repository-url>
    cd bigQuery-LLM-pipeline
    ```

2.  **Install the required Python libraries:**
    ```bash
    pip install google-cloud-bigquery google-generativeai
    ```

## Usage

The main pipeline logic is in the `src/cloud/main.py` script. To run the pipeline, execute this script:

```bash
python src/cloud/main.py
```

Before running, make sure to configure the following variables in the script to match your GCP environment:

*   `project_id`
*   `dataset_id`
*   `table`

## Project Structure

```
.
├── .gitignore
├── README.md
└── src
    ├── extract.py
    ├── load.py
    ├── transform.py
    └── cloud
        └── main.py
```

*   **`src/cloud/main.py`**: The main, self-contained script that orchestrates the entire ETL pipeline.
*   **`src/extract.py`**: A script to extract data from BigQuery.
*   **`src/transform.py`**: A script to transform the data using the Gemini LLM.
*   **`src/load.py`**: A script to load the transformed data back into BigQuery.
