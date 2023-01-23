import pandas as pd
import os.path as osp
import os
import numpy as np
import json
from tqdm import tqdm
from collections import Counter
import seaborn as sns
import matplotlib.pyplot as plt

def extract_from_json(json_dict):
    info_dict = {}
    info_dict["problemName"] = json_dict["about"]["problemName"]

    info_dict["taskKeywords"] = json_dict["about"]["taskKeywords"]
    info_dict["performanceMetrics"] = [_["metric"] for _ in json_dict["inputs"]["performanceMetrics"]]

    return info_dict


v_datasets_path = "viable_datasets.txt"

counter_tasks = Counter()
counter_metrics = Counter()

dict_shapes = {}

dict_df_metadata = {}


with open(v_datasets_path, "r") as fp:
    n_rows = int(fp.readline().strip())
    for idx, row in tqdm(enumerate(fp), total=n_rows):
        folder = row.strip()
        dataset_name = osp.basename(folder)
        dataset_folder = dataset_name + "_dataset"
        problem_folder = dataset_name + "_problem"

        pth = osp.join(folder, dataset_folder)
        tables_folder = osp.join(pth, "tables")
        total_shape = np.array([0,0])
        for df_file in os.listdir(tables_folder):
            df = pd.read_csv(osp.join(tables_folder, df_file))
            total_shape += df.shape

        json_metadata = json.load(open(
            osp.join(folder, problem_folder, "problemDoc.json")))
        info_metadata = extract_from_json(json_metadata)    

        counter_tasks.update(info_metadata["taskKeywords"])
        counter_metrics.update(info_metadata["performanceMetrics"])
        
        dict_shapes[dataset_name] = total_shape
        
        dict_df_metadata[dataset_name] = {
            "name": dataset_name,
            "taskKeywords": tuple(info_metadata["taskKeywords"]),
            "performanceMetrics": tuple(info_metadata["performanceMetrics"]),
            "totalShape": total_shape
        }
        
json.dump(dict_df_metadata, open("info_valid_datasets.json", "w"))

df_data = pd.DataFrame().from_dict(dict_shapes, orient="index", columns=["rows", "columns"])
df_tasks = pd.DataFrame().from_dict(counter_tasks, orient="index", columns=["count"])
df_metrics = pd.DataFrame().from_dict(counter_metrics, orient="index", columns=["count"])

fig, ax = plt.subplots(1)
sns.barplot(df_tasks.reset_index(), x="index", y="count")
_ = plt.xticks(rotation=45, ha="right")
plt.xlabel("Task")
plt.tight_layout()
fig.savefig("tasks.png")

fig, ax = plt.subplots(1)
sns.barplot(df_metrics.reset_index(), x="index", y="count", ax=ax)
_ = plt.xticks(rotation=45, ha="right")
plt.xlabel("Metric")
plt.tight_layout()
fig.savefig("metrics.png")

fig, ax = plt.subplots(1)
sns.scatterplot(data=df_data, x="rows", y="columns")
plt.xscale("log")
plt.yscale("log")
plt.title("Distribution of row/columns")
plt.xlabel("Number of rows (log)")
plt.ylabel("Number of columns (log)")
plt.tight_layout()
fig.savefig("dist-row-columns.png")