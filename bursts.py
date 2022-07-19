import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
import aiohttp
from netrc import netrc

import fsspec
import re
import numpy as np
import pandas as pd
import pystac
from shapely import geometry, wkt

# These constants are from the Sentinel-1 Level 1 Detailed Algorithm Definition PDF
# MPC Nom: DI-MPC-IPFDPM, MPC Ref: MPC-0307, Issue/Revision: 2/4, Table 9-7
NOMINAL_ORBITAL_DURATION = 12 * 24 * 3600 / 175
PREAMBLE_LENGTH = 2.299849
BEAM_CYCLE_TIME = 2.758273


def generate_path(safe, interior_path, protocol='zip://'):
    path = f'{protocol}*/{interior_path}::{safe}'
    return path


def fsspec_load_safe_json(slc, interior_path):
    with fsspec.open(generate_path(slc, interior_path), 'r') as f:
        xml = ET.parse(f)
    return xml.getroot()


def create_safe_path(safe_url, interior_path):
    safe = Path(safe_url).with_suffix('.SAFE').name
    path = Path(safe) / interior_path
    return str(path)


class SLCMetadata:
    def __init__(self, safe_url):
        self.safe_url = safe_url
        self.safe_name = Path(safe_url).with_suffix('.SAFE').name
        self.manifest, self.file_paths, self.annotation_paths, self.annotations = self.edl_download_metadata()

        self.measurement_paths = [x[2:] for x in self.file_paths if re.search('^\./measurement/s1.*tiff$', x)]
        self.measurement_paths.sort()

        self.relative_orbit = int(self.manifest.findall('.//{*}relativeOrbitNumber')[0].text)
        self.anx_time = float(self.manifest.find('.//{*}startTimeANX').text)
        self.n_swaths = len(self.manifest.findall('.//{*}swath'))

    @staticmethod
    def download_safe_xml(zip_fs, safe_url, interior_path):
        with zip_fs.open(create_safe_path(safe_url, 'manifest.safe')) as f:
            xml = ET.parse(f)
        return xml.getroot()

    def edl_download_metadata(self):
        my_netrc = netrc()
        username, _, password = my_netrc.authenticators('urs.earthdata.nasa.gov')
        auth = aiohttp.BasicAuth(username, password)

        storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}

        fs = fsspec.filesystem('https', **storage_options['https'])
        with fs.open(self.safe_url) as fo:
            safe = fsspec.filesystem('zip', fo=fo)
            manifest = self.download_safe_xml(safe, self.safe_url, 'manifest.safe')

            file_paths = [x.attrib['href'] for x in manifest.findall('.//fileLocation')]

            annotation_paths = [x[2:] for x in file_paths if re.search('^\./annotation/s1.*xml$', x)]
            annotation_paths.sort()
            annotations = [self.download_safe_xml(safe, self.safe_url, x) for x in annotation_paths]

        return manifest, file_paths, annotation_paths, annotations


class SwathMetadata:
    def __init__(self, slc, polarization, swath_index):
        self.safe_name = slc.safe_name
        self.polarization = polarization
        self.swath_index = swath_index
        self.annotation_path = [x for x in slc.annotation_paths if self.polarization.lower() in x][self.swath_index]
        self.measurement_path = [x for x in slc.measurement_paths if self.polarization.lower() in x][self.swath_index]
        annotation_index = slc.annotation_paths.index(self.annotation_path)

        self.annotation = slc.annotations[annotation_index]
        self.relative_orbit = slc.relative_orbit
        self.slc_anx_time = slc.anx_time

        self.n_bursts = int(self.annotation.find('.//{*}burstList').attrib['count'])
        self.gcp_df = self.create_gcp_df()

    @staticmethod
    def reformat_gcp(point):
        attribs = ['line', 'pixel', 'latitude', 'longitude', 'height']
        values = {}
        for attrib in attribs:
            values[attrib] = float(point.find(attrib).text)
        return values

    def create_gcp_df(self):
        points = self.annotation.findall('.//{*}geolocationGridPoint')
        gcp_df = pd.DataFrame([self.reformat_gcp(x) for x in points])
        gcp_df = gcp_df.sort_values(['line', 'pixel']).reset_index(drop=True)
        return gcp_df


class BurstMetadata:
    def __init__(self, swath, burst_index):
        self.safe_name = swath.safe_name
        self.polarization = swath.polarization
        self.swath_index = swath.swath_index
        self.burst_index = burst_index
        self.annotation_path = swath.annotation_path
        self.measurement_path = swath.measurement_path
        self.relative_orbit = swath.relative_orbit
        self.slc_anx_time = swath.slc_anx_time

        burst_annotations = swath.annotation.findall('.//{*}burst')
        byte_offset0 = int(burst_annotations[0].find('.//{*}byteOffset').text)
        byte_offset1 = int(burst_annotations[1].find('.//{*}byteOffset').text)
        self.burst_annotation = burst_annotations[burst_index]
        self.byte_offset = int(self.burst_annotation.find('.//{*}byteOffset').text)
        self.byte_length = byte_offset1 - byte_offset0

        self.lines = int(swath.annotation.find('.//{*}linesPerBurst').text)
        self.samples = int(swath.annotation.find('.//{*}samplesPerBurst').text)
        self.burst_anx_time = float(self.burst_annotation.find('.//{*}azimuthAnxTime').text)
        self.datetime = self.reformat_datetime()

        self.relative_burst_id = self.calculate_relative_burstid()
        self.stack_id = f'{self.relative_burst_id}_IW{self.swath_index + 1}'
        self.footprint, self.bounds = self.create_footprint(swath.gcp_df)
        self.absolute_burst_id = f'S1_SLC_{self.datetime}_{self.polarization.upper()}_{self.relative_burst_id}_IW{self.swath_index + 1}'

    def reformat_datetime(self):
        in_format = '%Y-%m-%dT%H:%M:%S.%f'
        out_format = '%Y%m%dT%H%M%S'
        dt = datetime.strptime(self.burst_annotation.find('.//{*}sensingTime').text, in_format)
        return dt.strftime(out_format)

    def calculate_relative_burstid(self):
        orbital = (self.relative_orbit - 1) * NOMINAL_ORBITAL_DURATION
        time_distance = self.burst_anx_time + orbital
        relative_burstid = 1 + np.floor((time_distance - PREAMBLE_LENGTH) / BEAM_CYCLE_TIME)
        # FIXME relative_burst_id is off by one
        return int(relative_burstid) + 1

    def create_footprint(self, gcp_df):
        first_line = gcp_df.loc[gcp_df['line'] == self.burst_index * self.lines, ['longitude', 'latitude']]
        second_line = gcp_df.loc[gcp_df['line'] == (self.burst_index + 1) * self.lines, ['longitude', 'latitude']]
        x1 = first_line['longitude'].tolist()
        y1 = first_line['latitude'].tolist()
        x2 = second_line['longitude'].tolist()
        y2 = second_line['latitude'].tolist()
        x2.reverse()
        y2.reverse()
        x = x1 + x2
        y = y1 + y2
        poly = geometry.Polygon(zip(x, y))
        return str(poly), poly.bounds

    def to_series(self):
        attribs = ['absolute_burst_id', 'relative_burst_id', 'datetime', 'footprint']
        attrib_dict = {k: getattr(self, k) for k in attribs}
        return pd.Series(attrib_dict)

    def to_stac_item(self):
        properties = {'lines': self.lines, 'samples': self.samples, 'byte_offset': self.byte_offset,
                      'byte_length': self.byte_length, 'stack_id': self.stack_id}
        href = f'{self.safe_name}/{self.measurement_path}'
        item = pystac.Item(id=self.absolute_burst_id,
                           geometry=self.footprint,
                           bbox=self.bounds,
                           datetime=datetime.strptime(self.datetime, "%Y%m%dT%H%M%S"),
                           properties=properties)

        item.add_asset(key=self.polarization.upper(),
                       asset=pystac.Asset(href=href, media_type=pystac.MediaType.GEOTIFF))
        return item


def generate_stac_catalog(safe_list):
    catalog = pystac.Catalog(id='burst-catalog', description='A catalog containing Sentinel-1 burst SLCs')
    stac_item_list = []

    for safe_path in safe_list:
        slc = SLCMetadata(safe_path)
        for swath_index in range(0, slc.n_swaths):
            swath = SwathMetadata(slc, 'vv', swath_index)
            for burst_index in range(0, swath.n_bursts):
                burst = BurstMetadata(swath, burst_index)

                stac_item_list.append(burst.to_stac_item())

    catalog.add_items(stac_item_list)

    return catalog


def save_stac_catalog_locally(catalog):
    stac_location = Path('.') / 'stac'
    if not stac_location.exists():
        stac_location.mkdir()
    catalog.normalize_hrefs(str(stac_location))
    catalog.make_all_asset_hrefs_relative()
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    return stac_location / 'catalog.json'


class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET')
        self.send_header('Cache-Control', 'no-store, no-cache, must-revalidate')
        return super(CORSRequestHandler, self).end_headers()


def initiate_stac_catalog_server(port, catalog_dir):
    port = port
    os.chdir(catalog_dir.resolve().__str__())
    url = f'http://localhost:{port}/catalog.json'
    print(f'{url}\n', 'In stac-browser run:\n', f'npm start -- --open --CATALOG_URL="{url}" ')

    with HTTPServer(('localhost', port), CORSRequestHandler) as httpd:
        httpd.serve_forever()


def read_local_burst(item, polarization='VV'):
    href = item.get_assets()[polarization].href
    safe, folder, tif = Path(href).parts
    safe = safe.replace('SAFE', 'zip')
    data_path = f'zip://*/{folder}/{tif}::./{safe}'

    with fsspec.open(data_path) as f:
        byte_string = fsspec.utils.read_block(f, offset=item.properties['byte_offset'],
                                              length=item.properties['byte_length'])

    arr = np.frombuffer(byte_string, dtype=np.int16).astype(float)
    fs_burst = arr.copy()
    fs_burst.dtype = 'complex'
    fs_burst = fs_burst.reshape((item.properties['lines'], item.properties['samples']))
    return fs_burst
