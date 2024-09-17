from flask import Flask, request, jsonify
import csv
from datetime import datetime

app = Flask(__name__)

csv_file_path = 'data.csv'

@app.route('/receive_data', methods=['POST'])
def receive_data():
    if request.method == 'POST':
        try:
            data = request.json

            if not isinstance(data, list):
                return jsonify({"error": "Data from API is not a list"}), 400

            for record in data:
                record['created_at'] = datetime.utcnow().isoformat()
                record['updated_at'] = datetime.utcnow().isoformat()

            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=record.keys())
                
                if file.tell() == 0:
                    writer.writeheader()

                writer.writerows(data)

            return jsonify({"message": "Data loaded successfully"}), 200

        except ValueError as e:
            return jsonify({"error": f"Data validation error: {e}"}), 400

        except Exception as e:
            return jsonify({"error": f"Unexpected error: {e}"}), 500
    else:
        return jsonify({"error": "Invalid request method"}), 405

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
