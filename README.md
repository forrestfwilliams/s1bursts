# Burst Workflow

A workflow for searching and accessing bursts that borrows heavily from the JPL sentinel-1 burst repository and the work of others at ASF. Here is the general workflow:

1. [sentinel1_burst_id](https://github.com/forrestfwilliams/sentinel1-burst-id) dataframe
2. dataframe to stac catalog
3. filter on catalog
4. request data present in filtered catalog
5. return as zarr file
