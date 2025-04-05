import json
import requests
import os

def main():
    # Create data directory if it doesn't exist
    os.makedirs("/app/data", exist_ok=True)
    
    # Load configuration
    with open("config.json", "r") as f:
        config = json.load(f)
    
    print("Starting scraper...")
    
    # Get data from each source
    all_data = []
    for source in config["data_sources"]:
        print(f"Fetching data from {source['name']}...")
        response = requests.get(source["url"])
        
        if response.status_code == 200:
            # Only take first 10 items to keep it simple
            data = response.json()[:10]
            all_data.extend(data)
            print(f"Successfully fetched {len(data)} items from {source['name']}")
        else:
            print(f"Failed to fetch data from {source['name']}")
    
    # Save raw data
    output_path = config["output_path"]
    with open(output_path, "w") as f:
        json.dump(all_data, f)
    
    print(f"Saved {len(all_data)} items to {output_path}")
    print("Scraper completed")

if __name__ == "__main__":
    main()