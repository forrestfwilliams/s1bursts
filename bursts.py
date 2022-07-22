import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from http.server import HTTPServer, SimpleHTTPRequestHandler
from netrc import netrc
from pathlib import Path
import xarray as xr

import aiohttp
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
from pqdm.threads import pqdm
from shapely import geometry, wkt
from shapely.ops import unary_union

# These constants are from the Sentinel-1 Level 1 Detailed Algorithm Definition PDF
# MPC Nom: DI-MPC-IPFDPM, MPC Ref: MPC-0307, Issue/Revision: 2/4, Table 9-7
NOMINAL_ORBITAL_DURATION = 12 * 24 * 3600 / 175
PREAMBLE_LENGTH = 2.299849
BEAM_CYCLE_TIME = 2.758273


class SLCMetadata:
    def __init__(self, safe_url, manifest, annotations):
        self.safe_url = safe_url
        self.manifest = manifest
        self.annotations = annotations
        self.safe_name = Path(safe_url).with_suffix('.SAFE').name

        self.file_paths = [x.attrib['href'] for x in self.manifest.findall('.//fileLocation')]
        self.measurement_paths = [x[2:] for x in self.file_paths if re.search('^\./measurement/s1.*tiff$', x)]
        self.measurement_paths.sort()

        self.relative_orbit = int(self.manifest.findall('.//{*}relativeOrbitNumber')[0].text)
        self.slc_anx_time = float(self.manifest.find('.//{*}startTimeANX').text)
        self.n_swaths = len(self.manifest.findall('.//{*}swath'))


class SwathMetadata:
    def __init__(self, slc, polarization, swath_index):
        self.safe_url = slc.safe_url
        self.safe_name = slc.safe_name
        self.relative_orbit = slc.relative_orbit
        self.slc_anx_time = slc.slc_anx_time
        self.polarization = polarization
        self.swath_index = swath_index

        pattern = f'^.*/s1.-iw{self.swath_index + 1}-slc-{self.polarization.lower()}.*$'
        self.annotation_path = [x for x in slc.annotations if re.search(pattern, x)][0]
        self.measurement_path = [x for x in slc.measurement_paths if re.search(pattern, x)][0]

        self.annotation = slc.annotations[self.annotation_path]

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
        self.safe_url = swath.safe_url
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
        return int(relative_burstid)

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
        footprint = geometry.Polygon(zip(x, y))
        return footprint, footprint.bounds

    def to_series(self):
        attribs = ['absolute_burst_id', 'relative_burst_id', 'datetime', 'footprint']
        attrib_dict = {k: getattr(self, k) for k in attribs}
        return pd.Series(attrib_dict)

    def to_stac_item(self):
        properties = {'lines': self.lines, 'samples': self.samples, 'byte_offset': self.byte_offset,
                      'byte_length': self.byte_length, 'stack_id': self.stack_id, 'safe_url': self.safe_url}
        href = f'{self.safe_name}/{self.measurement_path}'
        item = pystac.Item(id=self.absolute_burst_id,
                           geometry=geometry.mapping(self.footprint),
                           bbox=self.bounds,
                           datetime=datetime.strptime(self.datetime, "%Y%m%dT%H%M%S"),
                           properties=properties)

        item.add_asset(key=self.polarization.upper(),
                       asset=pystac.Asset(href=href, media_type=pystac.MediaType.GEOTIFF))
        return item


def create_safe_path(safe_url, interior_path):
    safe = Path(safe_url).with_suffix('.SAFE').name
    path = Path(safe) / interior_path
    return str(path)


def download_safe_xml(zip_fs, safe_url, interior_path):
    with zip_fs.open(create_safe_path(safe_url, interior_path)) as f:
        xml = ET.parse(f)
    return xml.getroot()


def get_netrc_auth():
    my_netrc = netrc()
    username, _, password = my_netrc.authenticators('urs.earthdata.nasa.gov')
    auth = aiohttp.BasicAuth(username, password)
    return auth


def edl_download_metadata(safe_url, auth):
    storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}

    http_fs = fsspec.filesystem('https', **storage_options['https'])
    with http_fs.open(safe_url) as fo:
        safe_zip = fsspec.filesystem('zip', fo=fo)
        manifest = download_safe_xml(safe_zip, safe_url, 'manifest.safe')

        file_paths = [x.attrib['href'] for x in manifest.findall('.//fileLocation')]
        annotation_paths = [x[2:] for x in file_paths if re.search('^\./annotation/s1.*xml$', x)]
        annotation_paths.sort()

        annotations = {x: download_safe_xml(safe_zip, safe_url, x) for x in annotation_paths}

    return manifest, annotations


def get_burst_metadata(safe_url_list, threads=None):
    auth = get_netrc_auth()

    if threads:
        args = [(safe_url, auth) for safe_url in safe_url_list]
        result = pqdm(args, edl_download_metadata, n_jobs=threads, argument_type="args")
        safe_metadata = {key: value for key, value in zip(safe_url_list, result)}
    else:
        safe_metadata = {x: edl_download_metadata(x, auth) for x in safe_url_list}

    bursts = []
    for safe_url in safe_url_list:
        manifest, annotations = safe_metadata[safe_url]
        slc = SLCMetadata(safe_url, manifest, annotations)
        for swath_index in range(0, slc.n_swaths):
            swath = SwathMetadata(slc, 'vv', swath_index)
            for burst_index in range(0, swath.n_bursts):
                burst = BurstMetadata(swath, burst_index)
                bursts.append(burst)

    return bursts


def generate_burst_stac_catalog(burst_list):
    catalog = pystac.Catalog(id='burst-catalog', description='A catalog containing Sentinel-1 burst SLCs')
    burst_items = [x.to_stac_item() for x in burst_list]
    # catalog.add_items(burst_items)
    stack_ids = set([x.properties['stack_id'] for x in burst_items])

    for stack_id in stack_ids:
        stack_items = [x for x in burst_items if x.properties['stack_id'] == stack_id]
        footprints = [geometry.Polygon(x.geometry['coordinates'][0]) for x in stack_items]
        datetimes = [x.datetime for x in stack_items]
        footprint = unary_union(footprints)
        date_min, date_max = min(datetimes), max(datetimes)

        spatial_extent = pystac.SpatialExtent(list(footprint.bounds))
        temporal_extent = pystac.TemporalExtent(intervals=[[date_min, date_max]])
        collection_extent = pystac.Extent(spatial=spatial_extent, temporal=temporal_extent)
        collection = pystac.Collection(id=stack_id,
                                       description=f'Sentinel-1 Burst Stack {stack_id}',
                                       extent=collection_extent)
        collection.add_items(stack_items)
        catalog.add_child(collection)

    return catalog


def burst_bytes_to_numpy(burst_bytes, shape):
    tmp_array = np.frombuffer(burst_bytes, dtype=np.int16).astype(float)
    array = tmp_array.copy()
    array.dtype = 'complex'
    array = array.reshape(shape)
    return array


def burst_numpy_to_xarray(item, array):
    n_lines, n_samples = item.properties['lines'], item.properties['samples']
    dims = ('time', 'line', 'sample')
    coords = ([item.datetime], list(range(n_lines)), list(range(n_samples)))
    coords = {key: value for key, value in zip(dims, coords)}
    burst_data_array = xr.DataArray(np.expand_dims(array, axis=0), coords=coords, dims=('time', 'line', 'sample'),
                                    attrs=item.properties)
    return burst_data_array


def edl_download_burst(item, auth, polarization='VV'):
    storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}

    http_fs = fsspec.filesystem('https', **storage_options['https'])
    with http_fs.open(item.properties['safe_url']) as fo:
        safe_zip = fsspec.filesystem('zip', fo=fo)
        with safe_zip.open(item.assets[polarization.upper()].href) as f:
            byte_string = fsspec.utils.read_block(f, offset=item.properties['byte_offset'],
                                                  length=item.properties['byte_length'])

    array = burst_bytes_to_numpy(byte_string, (item.properties['lines'], item.properties['samples']))
    burst_data_array = burst_numpy_to_xarray(item, array)
    return item.id, burst_data_array


def edl_download_stack(item_list, polarization='VV', threads=None):
    auth = get_netrc_auth()

    if threads:
        args = [(x, auth, polarization) for x in item_list]
        data_arrays = pqdm(args, edl_download_burst, n_jobs=threads, argument_type="args")
    else:
        data_arrays = [edl_download_burst(x, auth, polarization) for x in item_list]

    stack_dataset = xr.Dataset({k:v for k,v in data_arrays})
    return stack_dataset


def generate_burst_geodataframe(burst_list):
    burst_df = pd.DataFrame([x.to_series() for x in burst_list])
    footprint_geometry = burst_df['footprint'].map(wkt.loads)
    gdf = gpd.GeoDataFrame(burst_df.drop(columns=['footprint']), geometry=footprint_geometry, crs=4326)
    return gdf


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
