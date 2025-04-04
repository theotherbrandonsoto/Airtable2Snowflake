import pandas as pd
import json
import os
import glob
import tempfile

def translate_codes(codes_str, code_map):
    try:
        # The codes are already in JSON format from our export
        codes = json.loads(codes_str)
        # Translate each code using the mapping
        translated = [code_map.get(code, code) for code in codes]
        # Return as a JSON array string
        return json.dumps(translated) if translated else None
    except (json.JSONDecodeError, TypeError):
        return '[]'

def translate_subcategories(subcategories_str):
    try:
        # The subcategories are already translated in our export
        subcategories = json.loads(subcategories_str)
        # Return as a comma-separated string for Snowflake
        return ','.join(subcategories) if subcategories else None
    except (json.JSONDecodeError, TypeError):
        return None

def find_export_file():
    """Find the most recent airtable_export_final.csv file in temp directories"""
    # Look in temp directory
    temp_dir = tempfile.gettempdir()
    possible_paths = []
    
    # Search all subdirectories in temp for our file
    for root, dirs, files in os.walk(temp_dir):
        if 'airtable_export_final.csv' in files:
            path = os.path.join(root, 'airtable_export_final.csv')
            possible_paths.append((path, os.path.getmtime(path)))
    
    if possible_paths:
        # Return the most recently modified file
        return sorted(possible_paths, key=lambda x: x[1], reverse=True)[0][0]
    return None

def main():
    # Load the code mapping
    print("Loading code mapping...")
    with open('code_mapping.json', 'r') as f:
        code_map = json.load(f)
    
    # Create data directory if it doesn't exist
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)
    
    # Look for the export file
    local_export = os.path.join(data_dir, 'airtable_export_final.csv')
    temp_export = find_export_file()
    
    if os.path.exists(local_export):
        print(f"Using local export file: {local_export}")
        export_file = local_export
    elif temp_export:
        print(f"Found export file in temp directory: {temp_export}")
        export_file = temp_export
    else:
        raise FileNotFoundError("Could not find airtable_export_final.csv in data directory or temp directories")
    
    print(f"Processing export file: {export_file}")
    df = pd.read_csv(export_file)
    
    # Translate codes and subcategories
    print("Translating codes and subcategories...")
    df['Codes'] = df['Codes'].apply(lambda x: translate_codes(x, code_map) if pd.notnull(x) else '[]')
    df['Product Type Sub Category'] = df['Product Type Sub Category'].apply(lambda x: translate_subcategories(x) if pd.notnull(x) else None)
    
    # Handle Code Category field if it exists - keep as array
    if 'Code Category' in df.columns:
        df['Code Category'] = df['Code Category'].apply(lambda x: x if pd.notnull(x) else '[]')
    
    # Save the processed CSV
    output_file = os.path.join(data_dir, 'snowflake_import.csv')
    df.to_csv(output_file, index=False)
    print(f"Processed CSV saved to: {output_file}")
    
    # Print sample of translations for verification
    print("\nSample of translations (first 5 rows):")
    sample = df[['Complaint Number', 'Codes', 'Product Type Sub Category']].head()
    print(sample.to_string())
    
    # Print detailed example of Codes array format
    print("\nDetailed example of Codes array format:")
    first_code = df[df['Codes'] != '[]']['Codes'].iloc[0]
    print(f"Codes column example: {first_code}")
    print(f"Type: {type(first_code)}")
    
    if 'Code Category' in df.columns:
        print("\nCode Category array format:")
        first_category = df[df['Code Category'] != '[]']['Code Category'].iloc[0]
        print(f"Code Category example: {first_category}")
        print(f"Type: {type(first_category)}")

if __name__ == "__main__":
    main()