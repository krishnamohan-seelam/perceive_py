# Give an json file with schema with sample data  and generate synthetic data in table format having same schema based on number of rows as input
import json
import pandas as pd
import random
import string

def load_schema_from_json(json_file):
    """
    Load schema from a JSON file.

    :param json_file: Path to the JSON file containing the schema.
    :return: Parsed JSON schema.
    """
    with open(json_file, 'r') as file:
        schema = json.load(file)
    return schema

def generate_synthetic_data(schema, num_rows, seed=None):
    """
    Generate synthetic data based on the provided schema.

    :param schema: JSON schema defining the structure of the data.
    :param num_rows: Number of rows to generate.
    :param seed: Seed for random number generator to ensure reproducibility. Default is 42.
    :return: DataFrame containing synthetic data.
    """
    if seed is None:
        seed = 42  # Default seed value for reproducibility
    
    random.seed(seed)
    
    data = {}
    
    for field in schema['fields']:
        field_name = field['name']
        field_type = field['type']
        
        if field_type == 'string':
            data[field_name] = [
                ''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(num_rows)
            ]
        elif field_type == 'integer':
            data[field_name] = [random.randint(0, 100) for _ in range(num_rows)]
        elif field_type == 'float':
            data[field_name] = [random.uniform(0.0, 100.0) for _ in range(num_rows)]
        elif field_type == 'boolean':
            data[field_name] = [random.choice([True, False]) for _ in range(num_rows)]
        else:
            raise ValueError(f"Unsupported field type: {field_type}")
    return pd.DataFrame(data)

# Handle mapped columns with shared data
def handle_mapped_columns(field_name, field_type, num_rows, match_fraction, reverse_mapped_columns, shared_data):
    """
    Handle mapped columns with shared data.

    :param field_name: Name of the field being processed.
    :param field_type: Type of the field (e.g., 'string', 'integer', etc.).
    :param num_rows: Number of rows to generate.
    :param match_fraction: Fraction of rows where mapped columns should have matching data.
    :param reverse_mapped_columns: Reverse lookup dictionary for mapped columns.
    :param shared_data: Dictionary containing shared data for mapped columns.
    :return: List of generated data for the field.
    """
    source_column = reverse_mapped_columns.get(field_name)
    if source_column in shared_data:
        match_count = int(num_rows * match_fraction)
        matched_data = shared_data[source_column][:match_count]
        precomputed_choices = random.choices(shared_data[source_column], k=num_rows - match_count) if field_type in ['integer', 'boolean'] else None
        remaining_data = [
            precomputed_choices[i] if precomputed_choices and field_type in ['integer', 'boolean']
            else random.uniform(0.0, 100.0) if field_type == 'float'
            else ''.join(random.choices(string.ascii_letters + string.digits, k=10))
            for i in range(num_rows - match_count)
        ]
        return matched_data + remaining_data
    return None

def generate_combined_data(schemas, num_rows, mapped_source_and_target_columns=None, match_fraction=0.5):
    """
    Generate synthetic data for multiple schemas in a single function call.

    :param schemas: Dictionary with schema names as keys and dictionaries containing schema and seed as values.
    :param num_rows: Number of rows to generate.
    :param mapped_source_and_target_columns: Dictionary mapping columns between source and target data.
    :param match_fraction: Fraction of rows where mapped columns should have matching data. Default is 0.5.
    :return: Dictionary with schema names as keys and dictionaries containing schema and DataFrame as values.
    """
    result = {}
    shared_data = {}

    # Precompute reverse lookup dictionary for mapped columns
    reverse_mapped_columns = {tgt: src for src, tgt in mapped_source_and_target_columns.items()} if mapped_source_and_target_columns else {}

    for schema_name, schema_info in schemas.items():
        schema = schema_info["schema"]
        seed = schema_info["seed"]
        random.seed(seed)

        data = {}
        for field in schema['fields']:
            field_name = field['name']
            field_type = field['type']

            if mapped_source_and_target_columns and field_name in mapped_source_and_target_columns.values():
                generated_data = handle_mapped_columns(field_name, field_type, num_rows, match_fraction, reverse_mapped_columns, shared_data)
                if generated_data:
                    data[field_name] = generated_data
                    continue

            if field_type == 'string':
                generated_data = [
                    ''.join(random.choices(string.ascii_letters + string.digits, k=10)) for _ in range(num_rows)
                ]
            elif field_type == 'integer':
                generated_data = [random.randint(0, 100) for _ in range(num_rows)]
            elif field_type == 'float':
                generated_data = [random.uniform(0.0, 100.0) for _ in range(num_rows)]
            elif field_type == 'boolean':
                generated_data = [random.choice([True, False]) for _ in range(num_rows)]
            else:
                raise ValueError(f"Unsupported field type: {field_type}")

            data[field_name] = generated_data

            # Store shared data for mapped columns
            if mapped_source_and_target_columns and field_name in mapped_source_and_target_columns.keys():
                shared_data[field_name] = generated_data

        result[schema_name] = {
            "schema": schema,
            "table": pd.DataFrame(data)
        }

    return result

def create_sample_source_schema():
    """
    Create a sample source JSON schema.

    :return: Dictionary representing the source schema.
    """
    return {
        "fields": [
            {"name": "source_id", "type": "integer"},
            {"name": "source_name", "type": "string"},
            {"name": "source_value", "type": "float"},
            {'name':'source_boolean', 'type': 'boolean'}
        ]
    }

def create_sample_target_schema():
    """
    Create a sample target JSON schema.

    :return: Dictionary representing the target schema.
    """
    return {
        "fields": [
            {"name": "target_id", "type": "integer"},
            {"name": "target_name", "type": "string"},
            {"name": "target_value", "type": "float"},      
            {'name':'target_boolean', 'type': 'boolean'}
        ]
    }

# Example usage of the sample schemas
if __name__ == "__main__":
    source_schema = create_sample_source_schema()
    target_schema = create_sample_target_schema()
    num_rows = 10

    # Generate synthetic data using the sample schemas
    schemas = {
        "source": {"schema": source_schema, "seed": 42},
        "target": {"schema": target_schema, "seed": 13}
    }
    mapped_source_and_target_columns={"source_id": "target_id"}
    combined_data = generate_combined_data(
        schemas, num_rows, mapped_source_and_target_columns=mapped_source_and_target_columns
    )

    source_data = combined_data["source"]["table"]
    target_data = combined_data["target"]["table"]

    print("Source Data:")
    print(source_data)

    print("Target Data:")
    print(target_data)





