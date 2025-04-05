import json
import os
import time
import pandas as pd
from utils.excel_formatter import format_excel

def main():
    input_path = "/app/data/raw_data.json"
    output_json_path = "/app/data/transformed_data.json"
    output_excel_path = "/app/data/transformed_data.xlsx"
    
    # Wait for input file to be available
    max_retries = 10
    retries = 0
    
    while not os.path.exists(input_path) and retries < max_retries:
        print(f"Waiting for input file {input_path}...")
        time.sleep(2)
        retries += 1
    
    if not os.path.exists(input_path):
        print(f"Input file {input_path} not found after waiting. Exiting.")
        return
    
    print(f"Found input file {input_path}. Starting transformation...")
    
    # Load the data
    with open(input_path, "r") as f:
        data = json.load(f)
    
    # Transform the data (simple transformation for demo)
    transformed_data = []
    for item in data:
        transformed_item = {
            "id": item["id"],
            "title": item["title"].upper(),  # Convert title to uppercase
            "wordCount": len(item["body"].split()),  # Count words in body
            "summary": item["body"][:50] + "..." if len(item["body"]) > 50 else item["body"]  # Create summary
        }
        transformed_data.append(transformed_item)
    
    # Save transformed data as JSON
    with open(output_json_path, "w") as f:
        json.dump(transformed_data, f, indent=2)
    
    # Convert to DataFrame and save as Excel
    df = pd.DataFrame(transformed_data)
    format_excel(df, output_excel_path)
    
    print(f"Transformation complete. Processed {len(transformed_data)} items.")
    print(f"JSON output: {output_json_path}")
    print(f"Excel output: {output_excel_path}")

if __name__ == "__main__":
    main()