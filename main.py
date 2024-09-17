from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from datetime import datetime
import functions_framework
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize the BigQuery client
client = bigquery.Client()
table_id = 'arctic-column-432314-p8.customer_profiles.customer_data'

@functions_framework.http
def receive_data(request) -> jsonify:
    # Log incoming request headers
    logging.info("Headers: %s", request.headers)
    logging.info("Content-Type: %s", request.headers.get("Content-Type"))
    
    try:
        # Parsing JSON data from the request
        data = request.get_json()

        if not isinstance(data, list):
            return jsonify({"error": "Invalid data format, expected a list of records"}), 400
        
        # Adding timestamps to each record
        for record in data:
            record['created_at'] = datetime.utcnow().isoformat()
            record['updated_at'] = datetime.utcnow().isoformat()

        # Inserting data into BigQuery
        errors = client.insert_rows_json(table_id, data)
        if errors:
            logging.error("Failed to insert data: %s", errors)
            return jsonify({"error": f"Failed to insert data: {errors}"}), 500

        return jsonify({"message": "Data received and processed successfully"}), 200

    except ValueError as e:
        logging.error("Data validation error: %s", e)
        return jsonify({"error": f"Data validation error: {e}"}), 400

    except GoogleAPIError as e:
        logging.error("BigQuery API error: %s", e)
        return jsonify({"error": f"BigQuery API error: {e}"}), 500

    except Exception as e:
        logging.error("Unexpected error: %s", e)
        return jsonify({"error": f"Unexpected error: {e}"}), 500
