"""This script is needed to parse the metadata files (in format .json) and save
the relevant information in a pandas df for further analysis. 
"""
import pandas as pd
import json
import os.path as osp
import os
from copy import deepcopy
    
def extract_from_json(json_dict):
    this_info = deepcopy(info_fields)
    # this_info["datasetID"] = json_dict["about"]["datasetID"]
    key = json_dict["about"]["datasetID"]
    this_info["datasetName"] = json_dict["about"]["datasetName"]
    tot_columns = 0
    for resource in json_dict["dataResources"]:
        res_rtype = resource["resType"]
        this_info[res_rtype] += 1
        if resource["resType"] != "table":
            this_info["tabular_only"] = False
        else:
            tot_columns += resource["columnsCount"]
    this_info["tot_res"] = len(json_dict["dataResources"])
    this_info["tot_columns"] = tot_columns
    
    return key, {key: this_info}

DATA_PATH = "datasets-master"
TRAINING_DATASETS_FOLDER = osp.join(DATA_PATH, "training_datasets/LL0")
SEED_DATASETS_FOLDER = osp.join(DATA_PATH, "seed_datasets_current")

# SEED and TRAINING folders have different structure
subrepo = TRAINING_DATASETS_FOLDER


possible_dtypes = ["image","video","audio","speech","text","graph","edgeList","table","timeseries","raw"]

info_fields = {
    "datasetName": None,
    "tabular_only": True,
}
info_fields.update(zip(possible_dtypes,[0 for _ in possible_dtypes])) 

overall_stats_dict = {}

list_viable_datasets = []

for folder in os.listdir(subrepo):
    dataset_folder = folder + "_dataset"
    problem_folder = folder + "_problem"
    json_dataset = json.load(open(
        osp.join(subrepo, folder, dataset_folder, "datasetDoc.json")))
    dsetID, info_ = extract_from_json(json_dataset)
    if info_[dsetID]["tabular_only"]:
        list_viable_datasets.append(folder)        
    overall_stats_dict.update(info_)

df_stats = pd.DataFrame().from_dict(overall_stats_dict, orient="index")
print(df_stats.drop(["datasetName", "tot_res"], axis=1).sum())
print(df_stats["table"].max())