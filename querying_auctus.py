import os.path as osp
import os
import pandas as pd
from d3m.container import Dataset
import requests
import json
from tqdm import tqdm

viable_path = "viable_datasets.txt"
viable_datasets = []
with open(viable_path, "r") as fp:
    lines = [_.strip() for _ in fp.readlines()]
    num_lines = lines[0]
    viable_datasets = lines[1:]

good_responses = {}

for test_ds in tqdm(viable_datasets):
    # test_ds = viable_datasets[0]
    dsname = osp.basename(test_ds)
    metadata_path = osp.join(test_ds, dsname + "_dataset", "datasetDoc.json")
    # data_path = osp.join(test_ds, dsname + "_dataset", "tables/learningData.csv")
    mdata = json.load(open(metadata_path, "r"))

    dset_name = mdata["about"]["datasetName"]

    response = requests.post(
        "https://auctus.vida-nyu.org/api/v1/search",
        files={
            "query": json.dumps({"keywords": dset_name}).encode("utf-8"),
        },
    )
    response.raise_for_status()
    res_dict = response.json()
    if res_dict["total"] > 0:
        print(dsname, res_dict["total"])
        good_responses[dsname] = res_dict

json.dump(good_responses, open("good_responses.json", "w"))
