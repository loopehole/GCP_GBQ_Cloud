from google.cloud import bigquery

def modify_table_schema(project_id, dataset_id, table_id, new_schema_fields):
    client = bigquery.Client()

    # Create a fully qualified table reference
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
    
    # Get the existing table
    table = client.get_table(table_ref)
    
    # Add new schema fields
    existing_schema = table.schema
    new_schema = existing_schema + new_schema_fields
    
    # Update the table with the new schema
    table.schema = new_schema
    table = client.update_table(table, ["schema"])
    
    print(f"Table '{table_id}' schema updated successfully.")

# Example usage
project_id = 'your-project-id'
dataset_id = 'your-dataset-id'
table_id = 'your-table-id'

# Define new schema fields to add
new_schema_fields = [
    bigquery.SchemaField("new_field_name", "FIELD_TYPE"),
    # Add more fields as needed
]

modify_table_schema(project_id, dataset_id, table_id, new_schema_fields)
