from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError
from datetime import datetime

app = Flask(__name__)
client = bigquery.Client()
table_id = 'arctic-column-432314-p8.customer_profiles.customer_data'

@app.route('/receive_data', methods=['POST'])
def receive_data():
    try:
        # parsing JSON data from the request
        data = request.get_json()

        if not isinstance(data, list):
            return jsonify({"error": "Invalid data format, expected a list of records"}), 400
        
        # adding timestamps to each record
        for record in data:
            record['created_at'] = datetime.utcnow().isoformat()
            record['updated_at'] = datetime.utcnow().isoformat()

        # inserting data into BigQuery
        errors = client.insert_rows_json(table_id, data)
        if errors:
            return jsonify({"error": f"Failed to insert data: {errors}"}), 500

        return jsonify({"message": "Data received and processed successfully"}), 200

    except ValueError as e:
        return jsonify({"error": f"Data validation error: {e}"}), 400

    except GoogleAPIError as e:
        return jsonify({"error": f"BigQuery API error: {e}"}), 500

    except Exception as e:
        return jsonify({"error": f"Unexpected error: {e}"}), 500

# Cloud Function entry point
def flask_entry_point(request):
    return app(request)
