import os
import json
import requests
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from requests.exceptions import RequestException

def load_data(request):
    # defining API endpoint and BigQuery table
    api_url = 'http://lead.mobiletrk.net/api/leads'  # Correct API URL
    table_id = 'arctic-column-432314-p8.customer_profiles.customer_data'

    # here initializing BigQuery client
    client = bigquery.Client()

    try:
        # fetching data from an API
        response = requests.get(api_url)
        response.raise_for_status()  # it will raise HTTPError for the bad responses
        data = response.json()
        
        # validaitng data format -> if required we will adjust the data based on our schema
        if not isinstance(data, list):
            raise ValueError("Data from API is not a list")

        # inserting data into BigQuery
        errors = client.insert_rows_json(table_id, data)

        if errors:
            error_message = f"Encountered errors while inserting rows: {errors}"
            print(error_message)
            return error_message, 500

        return "Data loaded successfully", 200

    except RequestException as e:
        error_message = f"Request error: {e}"
        print(error_message)
        return error_message, 500

    except ValueError as e:
        error_message = f"Data validation error: {e}"
        print(error_message)
        return error_message, 400

    except GoogleAPIError as e:
        error_message = f"BigQuery API error: {e}"
        print(error_message)
        return error_message, 500

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        print(error_message)
        return error_message, 500
