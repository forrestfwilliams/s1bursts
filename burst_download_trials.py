from netrc import netrc
import aiohttp
import fsspec
import time
import sys


def get_netrc_auth():
    my_netrc = netrc()
    username, _, password = my_netrc.authenticators('urs.earthdata.nasa.gov')
    auth = aiohttp.BasicAuth(username, password)
    return auth


def http_burst(auth, url_path, interior_path, byte_offset, byte_length):
    storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}
    http_fs = fsspec.filesystem('https', **storage_options['https'])

    with http_fs.open(url_path) as http_f:
        zip_fs = fsspec.filesystem('zip', fo=http_f)
        with zip_fs.open(interior_path) as f:
            f.seek(byte_offset)
            burst_bytes = f.read(byte_length)

    return burst_bytes


def http_swath(auth, url_path, interior_path, byte_offset, byte_length):
    storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}
    http_fs = fsspec.filesystem('https', **storage_options['https'])

    with http_fs.open(url_path) as http_f:
        zip_fs = fsspec.filesystem('zip', fo=http_f)  # reads swath @40mb/s time=40s
        with zip_fs.open(interior_path) as f:
            swath_bytes = f.read()

    burst_bytes = swath_bytes[byte_offset:byte_offset + byte_length]
    return swath_bytes


def benchmark(auth, fun, dataset):
    start = time.time()
    result = fun(auth, **dataset)
    end = time.time()
    print(f'using {fun.__name__} downloaded {sys.getsizeof(result) * 1e-6:.2f}mb in {end - start:.2f}s')
    return result


if __name__ == '__main__':
    dataset1 = dict(
        url_path="https://datapool.asf.alaska.edu/SLC/SB/S1B_IW_SLC__1SDV_20210107T151555_20210107T151622_025050_02FB52_DB32.zip",
        interior_path="S1B_IW_SLC__1SDV_20210107T151555_20210107T151622_025050_02FB52_DB32.SAFE/measurement/s1b-iw2-slc-vv-20210107t151555-20210107t151621-025050-02fb52-005.tiff",
        byte_offset=109323,
        byte_length=152029824)

    dataset2 = dict(
        url_path="https://datapool.asf.alaska.edu/SLC/SB/S1B_IW_SLC__1SDV_20210107T151555_20210107T151622_025050_02FB52_DB32.zip",
        interior_path="S1B_IW_SLC__1SDV_20210107T151555_20210107T151622_025050_02FB52_DB32.SAFE/measurement/s1b-iw3-slc-vv-20210107t151556-20210107t151621-025050-02fb52-006.tiff",
        byte_offset=441755895,
        byte_length=147215404)

    dataset3 = dict(
        url_path="https://datapool.asf.alaska.edu/SLC/SB/S1B_IW_SLC__1SDV_20210212T151554_20210212T151621_025575_030C38_5BC5.zip",
        interior_path="S1B_IW_SLC__1SDV_20210212T151554_20210212T151621_025575_030C38_5BC5.SAFE/measurement/s1b-iw1-slc-vv-20210212t151556-20210212t151621-025575-030c38-004.tiff",
        byte_offset=147215404,
        byte_length=127429680)

    dataset4 = dict(
        url_path="https://datapool.asf.alaska.edu/SLC/SB/S1B_IW_SLC__1SDV_20210224T151554_20210224T151621_025750_0311EE_7026.zip",
        interior_path="S1B_IW_SLC__1SDV_20210224T151554_20210224T151621_025750_0311EE_7026.SAFE/measurement/s1b-iw1-slc-vv-20210224t151556-20210224t151621-025750-0311ee-004.tiff",
        byte_offset=1019497835,
        byte_length=127423672)

    auth = get_netrc_auth()

    _ = benchmark(auth, http_burst, dataset1)
    _ = benchmark(auth, http_burst, dataset2)
    _ = benchmark(auth, http_burst, dataset3)
    _ = benchmark(auth, http_burst, dataset4)

    print('')

    _ = benchmark(auth, http_swath, dataset1)
    _ = benchmark(auth, http_swath, dataset2)
    _ = benchmark(auth, http_swath, dataset3)
    _ = benchmark(auth, http_swath, dataset4)

    """
    Forrest's results
    using http_burst downloaded 152.03mb in 24.19s
    using http_burst downloaded 147.22mb in 97.83s
    using http_burst downloaded 127.43mb in 43.84s
    using http_burst downloaded 127.42mb in 186.28s

    using http_swath downloaded 1368.50mb in 43.25s
    using http_swath downloaded 1325.17mb in 137.98s
    using http_swath downloaded 1147.09mb in 39.45s
    using http_swath downloaded 1147.04mb in 36.05s
    """