import os

import requests
import time
import json
from dotenv import load_dotenv
from datasets_for_hierarchical_classification.functions import logging_config
import logging

load_dotenv()

BASE_URL = "https://api.openalex.org/works?page=1&filter=publication_year:2024,type:types/article,best_oa_location.is_published:true,open_access.is_oa:true,primary_topic.field.id:fields/33|fields/27|fields/22|fields/12|fields/17|fields/13|fields/11|fields/23|fields/31|fields/25|fields/14|fields/20|fields/32|fields/36|fields/16|fields/19|fields/28|fields/26|fields/24|fields/18|fields/21|fields/29|fields/30|fields/35|fields/15|fields/34,language:languages/en&sort=publication_date:asc&per_page=200"  # Replace with actual API URL

def fetch_all_documents():
    cursor = "IlsxNzA3NzgyNDAwMDAwLCA3OC4wLCAwLCAnaHR0cHM6Ly9vcGVuYWxleC5vcmcvVzQzOTE4MTUxNzknXSI="
    i = 2591
    batch_documents = []
    logging_config.create_log_file(f"get_big_openalex")

    # logging.info(f"Running into {device}")

    while True:
        url = f"{BASE_URL}&cursor={cursor}"
        logging.info(url)
        response = requests.get(url)
        data = response.json()

        cursor = data.get("meta", {}).get("next_cursor")
        logging.info(cursor)
        logging.info(i)

        if response.status_code != 200:
            logging.info(f"Error: {response.status_code}")
            break

        documents = data.get("results", [])  # Adjust based on API response structure

        if not documents:  # Stop if no more data
            break

        batch_documents.extend(documents)
        logging.info(f"Fetched page {i}, batch size: {len(batch_documents)}")

        # Save every 10 pages and reset batch
        if i % 10 == 0:
            save_documents(batch_documents, f"{os.getenv("DATASETS_FOLDER_BIG_OPENALEX_ORIGINAL_JSON")}/documents_{i}.json")
            batch_documents = []  # Reset batch after saving

            #time.sleep(10)
        i += 1  # Move to next page

def save_documents(documents, filename):
    """Helper function to save documents to a JSON file"""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(documents, f, indent=4)
    logging.info(f"Saved {len(documents)} documents to {filename}")

# Fetch documents and save to JSON
fetch_all_documents()