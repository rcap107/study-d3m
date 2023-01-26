from d3m import container
import datamart
import datamart_rest
import datetime
from pathlib import Path
import requests
import json
from tqdm import tqdm
import pandas as pd

VALID_PATH = Path("small_set.txt")


valid_paths = []
with open(VALID_PATH, "r") as fp:
    n_paths = int(fp.readline().strip())
    for idx, row in enumerate(fp):
        valid_paths.append(Path(row.strip()))

failed_datasets = []
# Number of candidates (unfiltered) for successful datasets
ok_datasets_candidates = {}
client = datamart_rest.RESTDatamart('https://auctus.vida-nyu.org/api/v1')
    
for v_path in tqdm(valid_paths):
    ds_name = v_path.stem
    target_dataset_learning_data = v_path/f"{ds_name}_dataset"/Path("tables/learningData.csv")
    assert target_dataset_learning_data.exists()
    df = pd.read_csv(target_dataset_learning_data)

    full_container = container.Dataset.load(target_dataset_learning_data.absolute().as_uri()) 
    
    try:
        cursor = client.search_with_data(query={}, supplied_data=full_container)
        results = cursor.get_next_page(limit=100, timeout=None)
        ok_datasets_candidates[ds_name] = len(results)
    except Exception as e:    
        print(f"Server error for {v_path}")
        failed_datasets.append(v_path)
    
