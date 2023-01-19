study-d3m
===
I created this repository to gather the scripts and notes I wrote while looking
into the [D3M data repository](https://datasets.datadrivendiscovery.org/d3m/datasets).

Info on the metadata can be found in [DatasetSchema.md](https://gitlab.com/datadrivendiscovery/data-supply/-/blob/shared/documentation/datasetSchema.md).

# Downloading the data
The readme in the repository advises to use [git lfs](https://git-lfs.github.com/), however I had a series of issues with this approach.  

If I try to use the command provided in the readme, I get a permission error:
```
$ git clone --recursive git@datasets.datadrivendiscovery.org:d3m/datasets.git
Cloning into 'datasets'...

git@datasets.datadrivendiscovery.org: Permission denied (publickey).
fatal: Could not read from remote repository.

Please make sure you have the correct access rights
and the repository exists.
```

Trying to clone in read-only mode with `https` runs into a different error: 
```$ git clone --recursive https://datasets.datadrivendiscovery.org/d3m/datasets.git

remote: Enumerating objects: 840547, done.
fatal: the remote end hung up unexpectedly.01 GiB | 6.52 MiB/s   
fatal: early EOF
fatal: index-pack failed
```
The repository is too big, and the git buffer runs out of space as soon as 1GB is reached. 

I am not sure what part of the process this is (indexing? fetching?), the point is that this issue prevents fetching, or any kind of partial operation on the repository, which als means that it is not possible to extract only a subset of the folders at a time.

In the end, I had to use `wget`:
```
$ wget https://datasets.datadrivendiscovery.org/d3m/datasets/-/archive/master/datasets-master.tar.bz2
```
This took a very long time as the DL speed was limited to ~500kb/s. I also made the mistake of downloading the file in `.tar` format, which will come into play later.

At least, I managed to download the full repository in the end, and it is now sitting on the storage unit of drago. 
# Studying metadata

# Studying data distribution

# Extracting tables of interest


