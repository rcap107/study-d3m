import tarfile
import os.path as osp
import os
from tqdm import tqdm
from collections import Counter

ARCHIVE_PATH = osp.expanduser("~/store/d3m/datasets-master.tar.bz2.1")

tfile = tarfile.open(ARCHIVE_PATH, mode="r:bz2")

member_names = tfile.getnames()
members = tfile.getmembers()

tgt_folders = [osp.join("datasets-master/training_datasets/LL0")]


exts = []
for folder in tgt_folders:
    for mname in member_names:
        if mname.startswith(folder):
            name, ext = osp.splitext(mname)
            exts.append(ext)
            
ext_counts = Counter(exts)
