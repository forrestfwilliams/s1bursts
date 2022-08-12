from datetime import datetime
from netrc import netrc
from pathlib import Path

import aiohttp
import numpy as np

# These constants are from the Sentinel-1 Level 1 Detailed Algorithm Definition PDF
# MPC Nom: DI-MPC-IPFDPM, MPC Ref: MPC-0307, Issue/Revision: 2/4, Table 9-7
NOMINAL_ORBITAL_DURATION = 12 * 24 * 3600 / 175
PREAMBLE_LENGTH = 2.299849
BEAM_CYCLE_TIME = 2.758273
SPEED_OF_LIGHT = 299792458.0
INTERNATIONAL_IDS = {'S1A': ' 2014-016A', 'S1B': '2016-025A'}
SCIHUB_USER = 'gnssguest'
SCIHUB_PASSWORD = 'gnssguest'


def get_netrc_auth(auth_cls=aiohttp.BasicAuth):
    my_netrc = netrc()
    username, _, password = my_netrc.authenticators('urs.earthdata.nasa.gov')
    auth = auth_cls(username, password)
    return auth


def convert_dt(dt_object):
    dt_format = '%Y-%m-%dT%H:%M:%S.%f'
    if isinstance(dt_object, str):
        dt = datetime.strptime(dt_object, dt_format)
    else:
        dt = dt_object.strftime(dt_format)
    return dt


def create_safe_path(safe_url, interior_path):
    safe = Path(safe_url).with_suffix('.SAFE').name
    path = Path(safe) / interior_path
    return str(path)


def burst_bytes_to_numpy(burst_bytes, shape):
    tmp_array = np.frombuffer(burst_bytes, dtype=np.int16).astype(float)
    array = tmp_array.copy()
    array.dtype = 'complex'
    array = array.reshape(shape).astype(np.csingle)
    return array
