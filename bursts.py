import os
import re
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from netrc import netrc
from pathlib import Path

import aiohttp
import fsspec
import geopandas as gpd
import numpy as np
import pandas as pd
import pystac
from pystac.extensions import sat, sar
import xarray as xr
from pqdm.threads import pqdm
from shapely import geometry, wkt
from shapely.ops import unary_union

# These constants are from the Sentinel-1 Level 1 Detailed Algorithm Definition PDF
# MPC Nom: DI-MPC-IPFDPM, MPC Ref: MPC-0307, Issue/Revision: 2/4, Table 9-7
NOMINAL_ORBITAL_DURATION = 12 * 24 * 3600 / 175
PREAMBLE_LENGTH = 2.299849
BEAM_CYCLE_TIME = 2.758273
SPEED_OF_LIGHT = 299792458.0


def convert_dt(dt_object):
    dt_format = '%Y-%m-%dT%H:%M:%S.%f'
    if isinstance(dt_object, str):
        dt = datetime.strptime(dt_object, dt_format)
    else:
        dt = dt_object.strftime(dt_format)
    return dt


class SLCMetadata:
    def __init__(self, safe_url, manifest, annotations):
        self.safe_url = safe_url
        self.manifest = manifest
        self.annotations = annotations
        self.safe_name = Path(safe_url).with_suffix('.SAFE').name
        self.platform = self.safe_name[0:3].upper()

        self.file_paths = [x.attrib['href'] for x in self.manifest.findall('.//fileLocation')]
        self.measurement_paths = [x[2:] for x in self.file_paths if re.search('^\./measurement/s1.*tiff$', x)]
        self.measurement_paths.sort()

        self.relative_orbit = int(self.manifest.findall('.//{*}relativeOrbitNumber')[0].text)
        self.absolute_orbit = int(self.manifest.findall('.//{*}orbitNumber')[0].text)
        self.polarizations = list({x.split('-')[3] for x in self.annotations.keys()})
        self.orbit_direction = self.manifest.findtext('.//{*}pass').lower()
        self.slc_start_anx = float(self.manifest.findtext('.//{*}startTimeANX'))
        self.n_swaths = len(self.manifest.findall('.//{*}swath'))

        self.iw2_mid_range = self.calculate_iw2_mid_range()

    def calculate_iw2_mid_range(self):
        iw2_annotation = [self.annotations[k] for k in self.annotations if 'iw2' in k][0]
        iw2_slant_range_time = float(iw2_annotation.findtext('.//{*}slantRangeTime'))
        iw2_n_samples = int(iw2_annotation.findtext('.//{*}samplesPerBurst'))
        iw2_starting_range = iw2_slant_range_time * SPEED_OF_LIGHT / 2
        iw2_range_sampling_rate = float(iw2_annotation.findtext('.//{*}rangeSamplingRate'))
        range_pxl_spacing = SPEED_OF_LIGHT / (2 * iw2_range_sampling_rate)
        iw2_mid_range = iw2_starting_range + 0.5 * iw2_n_samples * range_pxl_spacing
        return iw2_mid_range


class SwathMetadata:
    def __init__(self, slc, polarization, swath_index):
        if polarization.lower() not in slc.polarizations:
            raise (IndexError(f'There is no {polarization.lower()} polarization for this SLC'))
        self.polarization = polarization
        self.swath_index = swath_index

        attrs = ['safe_url', 'safe_name', 'absolute_orbit', 'relative_orbit', 'orbit_direction', 'iw2_mid_range',
                 'platform', 'slc_start_anx']
        [setattr(self, x, getattr(slc, x)) for x in attrs]

        pattern = f'^.*/s1.-iw{self.swath_index + 1}-slc-{self.polarization.lower()}.*$'
        self.annotation_path = [x for x in slc.annotations if re.search(pattern, x)][0]
        self.measurement_path = [x for x in slc.measurement_paths if re.search(pattern, x)][0]
        self.annotation = slc.annotations[self.annotation_path]

        self.n_bursts = int(self.annotation.find('.//{*}burstList').attrib['count'])
        self.radar_center_frequency = float(self.annotation.findtext('.//{*}radarFrequency'))
        self.wavelength = self.radar_center_frequency / SPEED_OF_LIGHT
        self.azimuth_steer_rate = float(self.annotation.findtext('.//{*}azimuthSteeringRate'))
        self.azimuth_time_interval = float(self.annotation.findtext('.//{*}azimuthTimeInterval'))
        self.slant_range_time = float(self.annotation.findtext('.//{*}slantRangeTime'))
        self.starting_range = self.slant_range_time * SPEED_OF_LIGHT / 2
        self.range_sampling_rate = float(self.annotation.findtext('.//{*}rangeSamplingRate'))
        self.range_pixel_spacing = SPEED_OF_LIGHT / 2 * self.range_sampling_rate
        self.range_bandwidth = float(self.annotation.findtext('.//{*}processingBandwidth'))
        self.range_window_type = self.annotation.findtext('.//{*}windowType').lower()
        self.range_window_coefficient = float(self.annotation.findtext('.//{*}windowCoefficient'))
        self.rank = int(self.annotation.findtext('.//{*}downlinkValues/rank'))
        self.prf_raw_data = float(self.annotation.findtext('.//{*}prf'))
        self.range_chirp_rate = float(self.annotation.findtext('.//{*}txPulseRampRate'))

        self.azimuth_frame_rates = self.get_polynomials('.//{*}azimuthFmRateList', 'azimuthFmRatePolynomial')
        self.dopplers = self.get_polynomials('.//{*}dcEstimateList', 'dataDcPolynomial')
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

    def get_polynomials(self, xml_pattern, poly_name):
        doppler_list_element = self.annotation.find(xml_pattern)
        polynomial_list = [self.parse_polynomial_element(x, poly_name) for x in doppler_list_element]
        polynomials = {k: v for k, v in polynomial_list}
        return polynomials

    @staticmethod
    def parse_polynomial_element(poly_element, poly_name):
        ref_time = poly_element.findtext('azimuthTime')

        half_c = 0.5 * SPEED_OF_LIGHT
        r0 = half_c * float(poly_element.findtext('t0'))
        coeffs = [float(x) for x in poly_element.findtext(poly_name).split()]
        poly1d_inputs = [coeffs, r0, half_c]  # inputs to isce3.core.Poly1d class
        return ref_time, poly1d_inputs


class BurstMetadata:
    def __init__(self, swath, burst_index):
        self.burst_index = burst_index
        attrs = ['absolute_orbit', 'annotation_path', 'azimuth_steer_rate', 'azimuth_time_interval', 'iw2_mid_range',
                 'measurement_path',
                 'orbit_direction', 'platform', 'polarization', 'prf_raw_data', 'radar_center_frequency',
                 'range_bandwidth', 'range_chirp_rate', 'range_pixel_spacing', 'range_sampling_rate',
                 'range_window_coefficient', 'range_window_type', 'rank', 'relative_orbit', 'safe_name', 'safe_url',
                 'slant_range_time', 'slc_start_anx', 'starting_range', 'swath_index', 'wavelength']
        [setattr(self, x, getattr(swath, x)) for x in attrs]

        burst_annotations = swath.annotation.findall('.//{*}burst')
        byte_offset0 = int(burst_annotations[0].findtext('.//{*}byteOffset'))
        byte_offset1 = int(burst_annotations[1].findtext('.//{*}byteOffset'))
        self.burst_annotation = burst_annotations[burst_index]
        self.byte_offset = int(self.burst_annotation.findtext('.//{*}byteOffset'))
        self.byte_length = byte_offset1 - byte_offset0

        self.lines = int(swath.annotation.findtext('.//{*}linesPerBurst'))
        self.samples = int(swath.annotation.findtext('.//{*}samplesPerBurst'))
        self.sensing_start = self.burst_annotation.findtext('.//{*}azimuthTime')
        self.burst_anx_delta = float(self.burst_annotation.find('.//{*}azimuthAnxTime').text)
        self.burst_anx = self.slc_start_anx + self.burst_anx_delta

        self.azimuth_frame_rate = self.get_nearest_polynomial(swath.azimuth_frame_rates)
        self.doppler = self.get_nearest_polynomial(swath.dopplers)
        self.first_valid_sample, self.last_valid_sample, self.first_valid_line, self.last_valid_line = self.get_lines_and_samples()

        self.relative_burst_id = self.calculate_relative_burstid()
        self.stack_id = f'{self.relative_burst_id}_IW{self.swath_index + 1}'
        self.opera_id = f't{self.relative_orbit}_{self.stack_id}'
        self.footprint, self.bounds, self.center = self.create_geometry(swath.gcp_df)
        reformatted_datetime = convert_dt(self.sensing_start).strftime('%Y%m%dT%H%M%S')
        self.absolute_burst_id = f'S1_SLC_{reformatted_datetime}_{self.polarization.upper()}_{self.relative_burst_id}_IW{self.swath_index + 1}'

    def get_nearest_polynomial(self, time_poly_pair):
        d_seconds = 0.5 * (self.lines - 1) * self.azimuth_time_interval
        t_mid = convert_dt(self.sensing_start) + timedelta(seconds=d_seconds)

        t_all = sorted([convert_dt(x) for x in time_poly_pair.keys()])

        # calculate 1st dt and polynomial
        t_start = t_all[0]
        dt = self.get_abs_dt(t_mid, t_start)
        nearest_poly = time_poly_pair[convert_dt(t_start)]

        # loop thru remaining time, polynomial pairs
        for t_iter in t_all[1:]:
            temp_dt = self.get_abs_dt(t_mid, t_iter)

            # stop looping if dt starts growing
            if temp_dt > dt:
                break

            # set dt and polynomial for next iteration
            dt, nearest_poly = temp_dt, time_poly_pair[convert_dt(t_iter)]

        return nearest_poly

    @staticmethod
    def get_abs_dt(t_mid, t_new):
        abs_dt = np.abs((t_mid - t_new).total_seconds())
        return abs_dt

    def calculate_relative_burstid(self):
        orbital = (self.relative_orbit - 1) * NOMINAL_ORBITAL_DURATION
        time_distance = self.burst_anx_delta + orbital
        relative_burstid = 1 + np.floor((time_distance - PREAMBLE_LENGTH) / BEAM_CYCLE_TIME)
        return int(relative_burstid)

    def create_geometry(self, gcp_df):
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
        return footprint, footprint.bounds, footprint.centroid.xy

    def get_lines_and_samples(self):
        first_valid_samples = [int(x) for x in self.burst_annotation.findtext('firstValidSample').split()]
        last_valid_samples = [int(x) for x in self.burst_annotation.findtext('lastValidSample').split()]

        first_valid_line = [x >= 0 for x in first_valid_samples].index(True)
        n_valid_lines = [x >= 0 for x in first_valid_samples].count(True)
        last_valid_line = first_valid_line + n_valid_lines - 1

        first_valid_sample = max(first_valid_samples[first_valid_line],
                                 first_valid_samples[last_valid_line])
        last_valid_sample = min(last_valid_samples[first_valid_line],
                                last_valid_samples[last_valid_line])

        return first_valid_sample, last_valid_sample, first_valid_line, last_valid_line,

    def to_series(self):
        attribs = ['absolute_burst_id', 'relative_burst_id', 'datetime', 'footprint']
        attrib_dict = {k: getattr(self, k) for k in attribs}
        return pd.Series(attrib_dict)

    def to_stac_item(self):
        internation_ids = {'S1A': ' 2014-016A', 'S1B': '2016-025A'}

        properties = {'stack_id': self.stack_id}
        for_opera = ['wavelength', 'azimuth_steer_rate', 'azimuth_time_interval', 'slant_range_time', 'starting_range',
                     'iw2_mid_range', 'range_sampling_rate', 'range_pixel_spacing', 'azimuth_frame_rate', 'doppler',
                     'range_bandwidth', 'opera_id', 'center', 'burst_index', 'first_valid_sample', 'last_valid_sample',
                     'first_valid_line', 'last_valid_line', 'range_window_type', 'range_window_coefficient', 'rank',
                     'prf_raw_data', 'range_chirp_rate']
        properties = properties | {k: getattr(self, k) for k in for_opera}

        asset_properties = {'lines': self.lines, 'samples': self.samples, 'byte_offset': self.byte_offset,
                            'byte_length': self.byte_length,
                            'interior_path': f'{self.safe_name}/{self.measurement_path}'}

        item = pystac.Item(id=self.absolute_burst_id,
                           geometry=geometry.mapping(self.footprint),
                           bbox=self.bounds,
                           datetime=convert_dt(self.sensing_start),
                           properties=properties)

        ext_sat = sat.SatExtension.ext(item, add_if_missing=True)
        ext_sat.apply(sat.OrbitState(self.orbit_direction), self.relative_orbit, self.absolute_orbit,
                      internation_ids[self.platform],
                      convert_dt(self.sensing_start))

        ext_sar = sar.SarExtension.ext(item, add_if_missing=True)
        ext_sar.apply('IW', sar.FrequencyBand('C'), [sar.Polarization(self.polarization.upper())], 'SLC-BURST',
                      self.radar_center_frequency, looks_range=1, looks_azimuth=1,
                      observation_direction=sar.ObservationDirection('right'))

        item.add_asset(key=self.polarization.upper(),
                       asset=pystac.Asset(href=self.safe_url, media_type=pystac.MediaType.GEOTIFF,
                                          extra_fields=asset_properties))
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


def local_read_metadata(zip_path):
    safe_name = Path(zip_path).with_suffix('.SAFE').name
    manifest_name = f'{safe_name}/manifest.safe'
    with zipfile.ZipFile(zip_path) as z:
        manifest = ET.parse(z.extract(manifest_name)).getroot()

        file_paths = [x.attrib['href'] for x in manifest.findall('.//fileLocation')]

        annotation_paths = [x[2:] for x in file_paths if re.search('^\./annotation/s1.*xml$', x)]
        annotation_paths.sort()
        annotations = {x: ET.parse(z.extract(f'{safe_name}/{x}')).getroot() for x in annotation_paths}
    return manifest, annotations


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
    n_lines, n_samples = array.shape
    properties = item.properties
    properties['id'] = item.id
    # TODO datetime as str
    properties['datetime'] = convert_dt(item.datetime)

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

    array = burst_bytes_to_numpy(burst_bytes, (lines, samples))
    burst_data_array = burst_numpy_to_xarray(item, array)
    return burst_data_array


def edl_download_stack(item_list, polarization='VV', threads=None):
    auth = get_netrc_auth()

    if threads:
        args = [(x, auth, polarization) for x in item_list]
        data_arrays = pqdm(args, edl_download_burst, n_jobs=threads, argument_type="args")
    else:
        data_arrays = [edl_download_burst(x, auth, polarization) for x in item_list]

    ids = [x.attrs['id'] for x in data_arrays]
    dates = [convert_dt(x.attrs['datetime']) for x in data_arrays]
    n_lines, n_samples = data_arrays[0].data.shape
    coords = {'time': dates, 'line': range(n_lines), 'sample': range(n_samples)}

    stack_dataset = xr.Dataset({k: v for k, v in zip(ids, data_arrays)}, coords=coords)
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
