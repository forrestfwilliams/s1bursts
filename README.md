# Burst Workflow

A workflow for searching and accessing bursts that borrows heavily from the JPL sentinel-1 burst repository and the work of others at ASF. Here is the general workflow:

1. Read SAFE metadata using fsspec over and EDL HTTPS connection
2. Create `BurstMetadata` objects that contain the metadata for each bursts
3. Turn a list of `BurstMetadata` objects into a STAC catalog
4. Filter on catalog
5. Request data present in filtered catalog over EDL HTTPS connection and return numpy arrays
