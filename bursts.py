import fnmatch
import json
import os
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

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


class SafeMetadata:
    def __init__(self, slc_path):
        self.manifest = fsspec_load_safe_json(slc_path, 'manifest.safe')

        self.file_paths = [x.attrib['href'] for x in self.manifest.findall('.//fileLocation')]
        self.relative_orbit = int(self.manifest.findall('.//{*}relativeOrbitNumber')[0].text)
        self.anx_time = float(self.manifest.find('.//{*}startTimeANX').text)
        self.n_swaths = len(self.manifest.findall('.//{*}swath'))

        self.annotation_paths = [x[2:] for x in self.file_paths if re.search('^\./annotation/s1.*xml$', x)]
        self.annotation_paths.sort()
        self.annotations = [fsspec_load_safe_json(slc_path, x) for x in self.annotation_paths]

        self.measurement_paths = [x[2:] for x in self.file_paths if re.search('^\./measurement/s1.*tiff$', x)]
        self.measurement_paths.sort()


class SwathMetadata:
    def __init__(self, safe, polarization, swath_index):
        self.polarization = polarization
        self.swath_index = swath_index
        self.annotation_path = [x for x in safe.annotation_paths if self.polarization.lower() in x][self.swath_index]
        self.measurement_path = [x for x in safe.measurement_paths if self.polarization.lower() in x][self.swath_index]
        annotation_index = safe.annotation_paths.index(self.annotation_path)

        self.annotation = safe.annotations[annotation_index]
        self.relative_orbit = safe.relative_orbit
        self.slc_anx_time = safe.anx_time

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
        self.footprint = self.create_footprint(swath.gcp_df)
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
        return poly

    def to_series(self):
        attribs = ['absolute_burst_id', 'relative_burst_id', 'datetime', 'polygon']
        attrib_dict = {k: getattr(self, k) for k in attribs}
        return pd.Series(attrib_dict)

    def to_stac_item(self):
        item = pystac.Item(id=self.absolute_burst_id,
                           geometry=self.footprint,
                           bbox=self.footprint.bounds,
                           datetime=datetime.strptime(self.datetime, "%Y%m%dT%H%M%S"),
                           properties={'stack_id': self.stack_id})

        item.add_asset(key=self.polarization.upper(),
                       asset=pystac.Asset(href=self.measurement_path, media_type=pystac.MediaType.GEOTIFF))
        return item


def generate_stac_catalog(df):
    catalog = pystac.Catalog(id='burst-catalog', description='A catalog containing Sentinel-1 burst SLCs')

    for i, row in df.iterrows():
        footprint = wkt.loads(row['geometry'])
        bbox = footprint.bounds
        timestamp = datetime.strptime(row['date'], "%Y%m%dT%H%M%S")
        location = {'lines': row['lines'], 'samples': row['samples'], 'byte_offset': row['byte_offset'],
                    'byte_length': row['byte_length']}

        item = pystac.Item(id=row['absoluteID'],
                           geometry=geometry.mapping(footprint),
                           bbox=bbox,
                           datetime=timestamp,
                           properties=location)
        item.add_asset(key=row['polarization'].upper(),
                       asset=pystac.Asset(href=row['measurement'], media_type=pystac.MediaType.GEOTIFF))
        catalog.add_item(item)

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
