import hashlib
import glob
import pandas as pd
import seaborn as sns
from pathlib import Path
from joblib import Parallel, delayed



def dump_table_to_csv(idx, table_path, dest_dir):
    try:
        tgt_dir, path = osp.split(osp.relpath(table_path, src_dir))
        path = osp.join(dest_dir, tgt_dir, osp.basename(path) + ".txt")
        if osp.exists(path):
            return (idx, 0)
        # Reading single table from parquet file
        tab = pq.read_table(table_path)
        # Converting table to csv passing from pandas, removing separators
        # and escape characters and forcing no quoting with QUOTE_NONE
        tab.to_pandas().to_csv(
            path, index=False, sep=" ", escapechar=" ", quoting=QUOTE_NONE
        )
        return (idx, 0)
    except Exception:
        # Avoid stopping the conversion procedure, count thefailures.
        return (idx, 1)

def hashing_datasets(dataset_list, n_jobs=1):
    n_jobs = 1
    r = Parallel(n_jobs=n_jobs, verbose=0)(
        delayed(dump_table_to_csv)(idx, table_path, dest_dir)
        for idx, table_path in tqdm(
            enumerate(list_paths), position=0, leave=False, total=len(list_paths)
        )
    )



data_path = Path("~/store/d3m/benchmark-datasets").expanduser()
assert data_path.exists()

all_datasets = glob.glob(f'{data_path}/**/learningData.csv', recursive=True)



