from d3m import container
import datamart
import datamart_rest
import datetime
from pathlib import Path
import requests
import json
from tqdm import tqdm
import pandas as pd

def reading_dataset_paths(VALID_PATH):
    valid_paths = []
    with open(VALID_PATH, "r") as fp:
        n_paths = int(fp.readline().strip())
        for idx, row in enumerate(fp):
            valid_paths.append(Path(row.strip()))
    return valid_paths


if __name__ == "__main__":
    VALID_PATH = Path("small_set.txt")
    query_limit = 100 # Maximum number of candidates to return when querying with a dataset
    query_timeout = None # Number of seconds to wait before timeout, this might become useful later. 

    valid_paths = reading_dataset_paths(VALID_PATH)

    failed_datasets = []
    # Number of candidates (unfiltered) for successful datasets
    results_by_dataset = {}

    # Connecting to the API
    client = datamart_rest.RESTDatamart('https://auctus.vida-nyu.org/api/v1')

        
    for v_path in tqdm(valid_paths):
        ds_name = v_path.stem
        target_dataset_learning_data = v_path/f"{ds_name}_dataset"/Path("tables/learningData.csv")
        assert target_dataset_learning_data.exists()

        # Loading the D3M representation
        full_container = container.Dataset.load(target_dataset_learning_data.absolute().as_uri()) 
        
        try:
            cursor = client.search_with_data(query={}, supplied_data=full_container)
            results = cursor.get_next_page(limit=100, timeout=None)
            results_by_dataset[ds_name] = results
        except Exception as e:    
            print(f"Server error for {v_path}")
            failed_datasets.append(v_path)
