# Burst Workflow

## Introduction
A workflow for searching and accessing bursts that borrows heavily from the JPL sentinel-1 burst repository and the work of others at ASF. Here is the general workflow:

1. Read SAFE metadata using fsspec over and EDL HTTPS connection
2. Create `BurstMetadata` objects that contain the metadata for each bursts
3. Turn a list of `BurstMetadata` objects into a STAC catalog
4. Filter on catalog
5. Request data present in filtered catalog over EDL HTTPS connection and return numpy arrays

Check out the `example.ipynb` notebook to try out the workflow.

## Installation
To install in Conda environment:
```bash
conda create -n tools_bursts
conda activate tools_bursts
conda install --file requirements.txt
```
in addition, you will need to install several JPL OPERA specific libraries:
```bash
conda install -c conda-forge isce3
conda install -c conda-forge backoff

git clone https://github.com/opera-adt/s1-reader.git
python -m pip install -e s1-reader

git clone https://github.com/opera-adt/compass.git
python -m pip install -e compass
```
