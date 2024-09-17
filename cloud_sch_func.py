import os
import json
import requests
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from requests.exceptions import RequestException
from jsonschema import validate, ValidationError
from flask import jsonify

def load_data(request):
    # Define API endpoint and BigQuery table
    api_url = 'https://api.example.com/data'  # Replace with your API endpoint
    table_id = 'arctic-column-432314-p8.customer_profiles.customer_data'

    # Initialize BigQuery client
    client = bigquery.Client()

    try:
        # Fetch data from API
        response = requests.get(api_url)
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = response.json()

        # Load JSON Schema
        with open('schema.json') as schema_file:
            schema = json.load(schema_file)

        # Validate data format
        if not isinstance(data, list):
            raise ValueError("Data from API is not a list")

        for record in data:
            try:
                validate(instance=record, schema=schema)
            except ValidationError as e:
                return jsonify({"error": f"Invalid data format: {str(e)}"}), 400

        # Insert data into BigQuery
        errors = client.insert_rows_json(table_id, data)

        if errors:
            error_message = f"Encountered errors while inserting rows: {errors}"
            print(error_message)
            return jsonify({"error": error_message}), 500

        return jsonify({"message": "Data loaded successfully"}), 200

    except RequestException as e:
        error_message = f"Request error: {e}"
        print(error_message)
        return jsonify({"error": error_message}), 500

    except ValueError as e:
        error_message = f"Data validation error: {e}"
        print(error_message)
        return jsonify({"error": error_message}), 400

    except GoogleAPIError as e:
        error_message = f"BigQuery API error: {e}"
        print(error_message)
        return jsonify({"error": error_message}), 500

    except json.JSONDecodeError as e:
        error_message = f"JSON decoding error: {e}"
        print(error_message)
        return jsonify({"error": error_message}), 500

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        print(error_message)
        return jsonify({"error": error_message}), 500
