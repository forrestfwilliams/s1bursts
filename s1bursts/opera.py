import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta

import fsspec
import isce3
import numpy as np
import requests
import s1reader
from osgeo import gdal
from shapely import geometry

from s1bursts import utils


def stac_item_to_opera_burst(item, polarization, orbit_dir, remote=False):
    import isce3
    import s1reader
    from s1reader import Sentinel1BurstSlc

    properties = item.properties
    asset = item.assets[polarization.upper()]
    asset_properties = asset.to_dict()

    # platform
    platform = \
        [k for k in utils.INTERNATIONAL_IDS if
         utils.INTERNATIONAL_IDS[k] == properties['sat:platform_international_designator']][0]

    # doppler
    shape = (asset_properties['lines'], asset_properties['samples'])
    doppler_poly1d = isce3.core.Poly1d(*properties['doppler'])
    doppler_lut2d = s1reader.s1_reader.doppler_poly1d_to_lut2d(doppler_poly1d,
                                                               properties['starting_range'],
                                                               properties['range_pixel_spacing'],
                                                               shape,
                                                               properties['azimuth_time_interval'])
    doppler = s1reader.s1_burst_slc.Doppler(doppler_poly1d, doppler_lut2d)

    # orbit
    orbit_path = s1reader.get_orbit_file_from_dir(asset.href.split('/')[-1], orbit_dir)
    with open(orbit_path, 'r') as f:
        orbit_xml = ET.parse(f)
    osv_list = orbit_xml.find('Data_Block/List_of_OSVs')
    sensing_duration = timedelta(seconds=shape[0] * properties['azimuth_time_interval'])
    orbit = s1reader.s1_reader.get_burst_orbit(item.datetime, item.datetime + sensing_duration, osv_list)

    args = dict(
        sensing_start=item.datetime,
        radar_center_frequency=properties['sar:center_frequency'],
        wavelength=properties['wavelength'],
        azimuth_steer_rate=properties['azimuth_steer_rate'],
        azimuth_time_interval=properties['azimuth_time_interval'],
        slant_range_time=properties['slant_range_time'],
        starting_range=properties['starting_range'],
        iw2_mid_range=properties['iw2_mid_range'],
        range_sampling_rate=properties['range_sampling_rate'],
        range_pixel_spacing=properties['range_pixel_spacing'],
        shape=shape,
        azimuth_fm_rate=isce3.core.Poly1d(*properties['azimuth_frame_rate']),
        doppler=doppler,
        range_bandwidth=properties['range_bandwidth'],
        polarization=polarization,
        burst_id=properties['opera_id'],
        platform_id=platform,
        center=geometry.Point(properties['center']),
        border=list(item.geometry['coordinates'][0]),
        orbit=orbit,
        orbit_direction=properties['sat:orbit_state'].capitalize(),
        tiff_path=asset.href,
        i_burst=properties['burst_index'],
        first_valid_sample=properties['first_valid_sample'],
        last_valid_sample=properties['last_valid_sample'],
        first_valid_line=properties['first_valid_line'],
        last_valid_line=properties['last_valid_line'],
        range_window_type=properties['range_window_type'].capitalize(),
        range_window_coefficient=properties['range_window_coefficient'],
        rank=properties['rank'],
        prf_raw_data=properties['prf_raw_data'],
        range_chirp_rate=properties['range_chirp_rate'],
    )

    remote_args = dict(
        absolute_id=item.id,
        byte_offset=asset_properties['byte_offset'],
        byte_length=asset_properties['byte_length'],
        interior_path=asset_properties['interior_path'],
        url_path=asset.href,
    )

    if remote:
        all_args = args | remote_args
        all_args['tiff_path'] = ''
        opera_burst = RemoteSentinel1BurstSLC(**all_args)
    else:
        opera_burst = Sentinel1BurstSlc(**args)
    return opera_burst


def cmr_to_opera_burst(cmr_url, remote=False):
    burst_response = json.loads(requests.get(cmr_url).content)['items'][0]
    properties = attribute_dict = {x['Name']: x['Values'][0] for x in burst_response['umm']['AdditionalAttributes']}
    sensing_start = datetime.fromisoformat(
        burst_response['umm']['TemporalExtent']['RangeDateTime']['BeginningDateTime']).replace(tzinfo=None)
    shape = (int(properties['LINES']), int(properties['SAMPLES']))
    center = geometry.Point(float(properties['CENTER_LON']), float(properties['CENTER_LAT']))

    # boundary
    point_dict = \
        burst_response['umm']['SpatialExtent']['HorizontalSpatialDomain']['Geometry']['GPolygons'][0]['Boundary'][
            'Points']
    border = [[x['Longitude'], x['Latitude']] for x in point_dict]

    # doppler
    doppler_poly1d = isce3.core.Poly1d(*json.loads(properties['DOPPLER']))
    doppler_lut2d = s1reader.s1_reader.doppler_poly1d_to_lut2d(doppler_poly1d,
                                                               float(properties['STARTING_RANGE']),
                                                               float(properties['RANGE_PIXEL_SPACING']),
                                                               shape,
                                                               float(properties['AZIMUTH_TIME_INTERVAL']))
    doppler = s1reader.s1_burst_slc.Doppler(doppler_poly1d, doppler_lut2d)

    # orbit
    sensor_id, _, start_time, end_time, _ = s1reader.s1_orbit.parse_safe_filename(properties['SAFE_NAME'])
    orbit_dict = s1reader.s1_orbit.get_orbit_dict(sensor_id, start_time, end_time, 'AUX_POEORB')
    if orbit_dict is None:
        orbit_dict = s1reader.s1_orbit.get_orbit_dict(sensor_id, start_time, end_time, 'AUX_RESORB')

    response = requests.get(url=orbit_dict['orbit_url'], auth=(utils.SCIHUB_USER, utils.SCIHUB_PASSWORD))
    osv_list = ET.fromstring(response.content).find('Data_Block/List_of_OSVs')
    sensing_duration = timedelta(seconds=shape[0] * float(properties['AZIMUTH_TIME_INTERVAL']))
    orbit = s1reader.s1_reader.get_burst_orbit(sensing_start, sensing_start + sensing_duration, osv_list)

    args = dict(
        sensing_start=sensing_start,
        radar_center_frequency=float(properties['RADAR_CENTER_FREQUENCY']),
        wavelength=float(properties['WAVELENGTH']),
        azimuth_steer_rate=float(properties['AZIMUTH_STEER_RATE']),
        azimuth_time_interval=float(properties['AZIMUTH_TIME_INTERVAL']),
        slant_range_time=float(properties['SLANT_RANGE_TIME']),
        starting_range=float(properties['STARTING_RANGE']),
        iw2_mid_range=float(properties['IW2_MID_RANGE']),
        range_sampling_rate=float(properties['RANGE_SAMPLING_RATE']),
        range_pixel_spacing=float(properties['RANGE_PIXEL_SPACING']),
        shape=shape,
        azimuth_fm_rate=isce3.core.Poly1d(*json.loads(properties['AZIMUTH_FRAME_RATE'])),
        doppler=doppler,
        range_bandwidth=float(properties['RANGE_BANDWIDTH']),
        polarization=properties['POLARIZATION'],
        burst_id=properties['OPERA_ID'],
        platform_id=properties['SAFE_NAME'][:3],
        center=center,
        border=border,
        orbit=orbit,
        orbit_direction=properties['ASCENDING_DESCENDING'],
        tiff_path='',
        i_burst=int(properties['BURST_INDEX']),
        first_valid_sample=int(properties['FIRST_VALID_SAMPLE']),
        last_valid_sample=int(properties['LAST_VALID_SAMPLE']),
        first_valid_line=int(properties['FIRST_VALID_LINE']),
        last_valid_line=int(properties['LAST_VALID_LINE']),
        range_window_type=properties['RANGE_WINDOW_TYPE'].capitalize(),
        range_window_coefficient=float(properties['RANGE_WINDOW_COEFFICIENT']),
        rank=int(properties['RANK']),
        prf_raw_data=float(properties['PRF_RAW_DATA']),
        range_chirp_rate=float(properties['RANGE_CHIRP_RATE']),
    )

    remote_args = dict(
        absolute_id=properties['GROUP_ID'],
        byte_offset=int(properties['BYTE_OFFSET']),
        byte_length=int(properties['BYTE_LENGTH']),
        interior_path=f'{properties["SAFE_NAME"]}/{properties["MEASUREMENT_PATH"]}',
        url_path=properties['SAFE_URL'],
    )

    if remote:
        all_args = args | remote_args
        opera_burst = RemoteSentinel1BurstSLC(**all_args)
    else:
        opera_burst = s1reader.Sentinel1BurstSlc(**args)
    return opera_burst


# need to change Sentinel1BurstSLC to unfrozen as well
@dataclass(frozen=False)
class RemoteSentinel1BurstSLC(s1reader.Sentinel1BurstSlc):
    absolute_id: str
    byte_offset: int
    byte_length: int
    interior_path: str
    url_path: str

    def edl_download_data(self):
        auth = utils.get_netrc_auth()
        storage_options = {'https': {'client_kwargs': {'trust_env': True, 'auth': auth}}}

        http_fs = fsspec.filesystem('https', **storage_options['https'])
        with http_fs.open(self.url_path) as http_f:
            zip_fs = fsspec.filesystem('zip', fo=http_f)
            with zip_fs.open(self.interior_path) as f:
                f.seek(self.byte_offset)
                burst_bytes = f.read(self.byte_length)

        return utils.burst_bytes_to_numpy(burst_bytes, self.shape)

    def slc_to_file(self, out_path, fmt='ENVI'):
        self.tiff_path = str(out_path)
        array = self.edl_download_data()
        driver = gdal.GetDriverByName(fmt)
        n_rows, n_cols = array.shape
        out_dataset = driver.Create(self.tiff_path, n_cols, n_rows, 1, gdal.GDT_CFloat32)
        out_dataset.GetRasterBand(1).WriteArray(array)
        out_dataset = None

    def slc_to_vrt_file(self, out_path):
        raise NotImplementedError('This method is not valid for Remote SLC objects')


def georeference_burst(burst_instance, dem_path, output_dir, scratch_dir):
    from compass.utils.geo_grid import generate_geogrids
    # set options
    threshold = 1e-08
    iters = 25
    blocksize = 1000
    flatten = True
    output_format = 'GTiff'
    geocoding_dict = dict(output_format='GTiff',
                          flatten=True,
                          lines_per_block=1000,
                          output_epsg=None,
                          x_posting=None,
                          y_posting=None,
                          x_snap=None,
                          y_snap=None,
                          top_left=dict(x=None, y=None),
                          bottom_right=dict(x=None, y=None))
    geogrids = generate_geogrids([burst_instance], geocoding_dict, dem_path)

    # prep data
    date_str = burst_instance.sensing_start.strftime("%Y%m%d")
    burst_id = burst_instance.burst_id
    pol = burst_instance.polarization
    geo_grid = geogrids[burst_id]

    # make dirs
    os.makedirs(output_dir, exist_ok=True)
    scratch_path = f'{scratch_dir}/{burst_id}/{date_str}'
    os.makedirs(scratch_path, exist_ok=True)

    temp_slc_path = f'{scratch_dir}/{burst_id}_{pol}_temp.slc'
    burst_instance.slc_to_file(temp_slc_path)
    rdr_burst_raster = isce3.io.Raster(temp_slc_path)
    print('data downloaded...')

    # Run Geo-referencing
    dem_raster = isce3.io.Raster(dem_path)
    epsg = dem_raster.get_epsg()
    proj = isce3.core.make_projection(epsg)
    ellipsoid = proj.ellipsoid
    image_grid_doppler = isce3.core.LUT2d()

    radar_grid = burst_instance.as_isce3_radargrid()
    native_doppler = burst_instance.doppler.lut2d
    orbit = burst_instance.orbit

    # Get azimuth polynomial coefficients for this burst
    az_carrier_poly2d = burst_instance.get_az_carrier_poly()

    # Generate output geocoded burst raster
    out_name = f'{output_dir}/{burst_id}_{date_str}_{pol}.tif'
    geo_burst_raster = isce3.io.Raster(
        out_name,
        geo_grid.width, geo_grid.length,
        rdr_burst_raster.num_bands, gdal.GDT_CFloat32, output_format)

    # Extract burst boundaries
    b_bounds = np.s_[burst_instance.first_valid_line:burst_instance.last_valid_line,
               burst_instance.first_valid_sample:burst_instance.last_valid_sample]

    # Create sliced radar grid representing valid region of the burst
    sliced_radar_grid = burst_instance.as_isce3_radargrid()[b_bounds]

    # Geocode
    isce3.geocode.geocode_slc(geo_burst_raster, rdr_burst_raster,
                              dem_raster,
                              radar_grid, sliced_radar_grid,
                              geo_grid, orbit,
                              native_doppler,
                              image_grid_doppler, ellipsoid, threshold,
                              iters, blocksize, flatten,
                              azimuth_carrier=az_carrier_poly2d)

    # Set geo transformation
    geotransform = [geo_grid.start_x, geo_grid.spacing_x, 0,
                    geo_grid.start_y, 0, geo_grid.spacing_y]
    geo_burst_raster.set_geotransform(geotransform)
    geo_burst_raster.set_epsg(epsg)
    del geo_burst_raster
    print('geo-referenced!')
    return out_name
