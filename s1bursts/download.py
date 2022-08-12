import fsspec
import xarray as xr
from pqdm.threads import pqdm

from s1bursts import utils


def burst_numpy_to_xarray(item, array):
    n_lines, n_samples = array.shape
    properties = item.properties
    properties['id'] = item.id
    # TODO datetime as str
    properties['datetime'] = utils.convert_dt(item.datetime)

    dims = ('line', 'sample')
    coords = (range(n_lines), range(n_samples))
    coords = {key: value for key, value in zip(dims, coords)}

    burst_data_array = xr.DataArray(array, coords=coords, dims=dims, attrs=properties)
    return burst_data_array


def edl_download_burst(item, auth, polarization='VV'):
    asset = item.assets[polarization].to_dict()
    lines, samples = asset['lines'], asset['samples']
    byte_offset, byte_length = asset['byte_offset'], asset['byte_length']
    storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}

    http_fs = fsspec.filesystem('https', **storage_options['https'])
    with http_fs.open(asset['href']) as http_f:
        zip_fs = fsspec.filesystem('zip', fo=http_f)
        with zip_fs.open(asset['interior_path']) as f:
            # burst_bytes = f.read()[byte_offset:byte_offset + byte_length]  # downloads swath @40mb/s

            f.seek(byte_offset)  # downloads burst @15mb/s
            burst_bytes = f.read(byte_length)

    array = utils.burst_bytes_to_numpy(burst_bytes, (lines, samples))
    burst_data_array = burst_numpy_to_xarray(item, array)
    return burst_data_array


def edl_download_stack(item_list, polarization='VV', threads=None):
    auth = utils.get_netrc_auth()

    if threads:
        args = [(x, auth, polarization) for x in item_list]
        data_arrays = pqdm(args, edl_download_burst, n_jobs=threads, argument_type="args")
    else:
        data_arrays = [edl_download_burst(x, auth, polarization) for x in item_list]

    ids = [x.attrs['id'] for x in data_arrays]
    dates = [utils.convert_dt(x.attrs['datetime']) for x in data_arrays]
    n_lines, n_samples = data_arrays[0].data.shape
    coords = {'time': dates, 'line': range(n_lines), 'sample': range(n_samples)}

    stack_dataset = xr.Dataset({k: v for k, v in zip(ids, data_arrays)}, coords=coords)
    return stack_dataset
