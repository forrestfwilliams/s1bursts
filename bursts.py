import datetime
import fnmatch
import json
import os
import xml.etree.ElementTree as ET
import zipfile
from datetime import datetime, timedelta
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import fsspec
import numpy as np
import pandas as pd
import pystac
from shapely import geometry, wkt

# These constants are from the Sentinel-1 Level 1 Detailed Algorithm Definition PDF
# MPC Nom: DI-MPC-IPFDPM, MPC Ref: MPC-0307, Issue/Revision: 2/4, Table 9-7
NOMINAL_ORBITAL_DURATION = timedelta(seconds=12 * 24 * 3600 / 175)
PREAMBLE_LENGTH_IW = timedelta(seconds=2.299849)
PREAMBLE_LENGTH_EW = timedelta(seconds=2.299970)
BEAM_CYCLE_TIME_IW = timedelta(seconds=2.758273)
BEAM_CYCLE_TIME_EW = timedelta(seconds=3.038376)


def getxmlattr(xml_root, path, key):
    """
    Function to extract the attribute of an xml key
    """

    try:
        res = xml_root.find(path).attrib[key]
    except:
        raise Exception('Cannot find attribute %s at %s' % (key, path))

    return res


def getxmlvalue(xml_root, path):
    """
    Function to extract value in the xml for a given path
    """

    try:
        res = xml_root.find(path).text
    except:
        raise Exception('Tag= %s not found' % (path))

    if res is None:
        raise Exception('Tag = %s not found' % (path))

    return res


def getxmlelement(xml_root, path):
    """
    extract an element of a xml file
    """

    try:
        res = xml_root.find(path)
    except:
        raise Exception('Cannot find path %s' % (path))

    if res is None:
        raise Exception('Cannot find path %s' % (path))

    return res


def read_time(input_str, fmt="%Y-%m-%dT%H:%M:%S.%f"):
    """
    The function to convert a string to a datetime object
    Parameters:
        input_str: A string which contains the data time with the format of fmt
        fmt: the format of input_str

    Returns:
        dt: python's datetime object
    """

    dt = datetime.strptime(input_str, fmt)
    return dt


def getCoordinates(zipname, swath, polarization):
    """
    The function to extract the Ground Control Points (GCP) of bursts from tiff file.

    Parameters:
        zipname: the name of the zipfile which contains the data

    Returns:
        df_coordinates: A pandas dataframe of GCPs
    """

    zf = zipfile.ZipFile(zipname, 'r')

    tiffpath = os.path.join('*SAFE', 'measurement', 's1[ab]-iw{}-slc-{}*tiff'.format(swath, polarization))
    match = fnmatch.filter(zf.namelist(), tiffpath)
    zf.close()

    tiffname = os.path.join('/vsizip/' + zipname, match[0])
    cmd = "gdalinfo -json {} >> info.json".format(tiffname)
    os.system(cmd)
    with open("info.json", 'r') as fid:
        info = json.load(fid)

    df_coordinates = pd.DataFrame(info['gcps']['gcpList'])
    os.system('rm info.json')
    return df_coordinates, match[0]


def burstCoords(geocoords, lineperburst, idx):
    """
    The function to extract coordinates for a given burst.

    Parameters:
        geocoords: A pandas dataframe of GCPs
        lineperburst: number of lines in each burst
        idx: index of the burst of interest

    Returns:
        poly: a shapely polygon represnting the boundary of the burst
        xc: longitude of the centroid of the polygon
        yc: latitude of the centroid of the polygon
    """

    firstLine = geocoords.loc[geocoords['line'] == idx * lineperburst].filter(['x', 'y'])
    secondLine = geocoords.loc[geocoords['line'] == (idx + 1) * lineperburst].filter(['x', 'y'])
    X1 = firstLine['x'].tolist()
    Y1 = firstLine['y'].tolist()
    X2 = secondLine['x'].tolist()
    Y2 = secondLine['y'].tolist()
    X2.reverse()
    Y2.reverse()
    X = X1 + X2
    Y = Y1 + Y2
    poly = geometry.Polygon(zip(X, Y))
    xc, yc = poly.centroid.xy
    return poly, xc[0], yc[0]


# These formulas are from the abovementioned PDF, section 9.25
def calc_burstid_from_timedist(timedist: timedelta, is_ew: bool = False) -> int:
    prelen = PREAMBLE_LENGTH_EW if is_ew else PREAMBLE_LENGTH_IW
    beamtime = BEAM_CYCLE_TIME_EW if is_ew else BEAM_CYCLE_TIME_IW
    burstid = 1 + np.floor((timedist - prelen) / beamtime)
    return burstid


def calc_timedistance(mid_burst_datetime: datetime, anx_datetime: datetime, orbit_number: int) -> timedelta:
    orbital = (orbit_number - 1) * NOMINAL_ORBITAL_DURATION
    time_distance = mid_burst_datetime - anx_datetime + orbital
    return time_distance


def calculate_relative_burst_id(mid_burst_time: str, anx_time: str, orbit_number: int, is_ew: bool = False) -> int:
    """
    Calculates the burst ID of a SLC granule based on the various inputs.
    :param mid_burst_time: ISO date from annotation XML /product/swathTiming/burstList/burst/sensingTime
    :param anx_time: ISO date, found in manifest.safe file <s1:ascendingNodeTime>
    :param orbit_number: Can be either relative or absolute orbit number.
                         From manifest.safe <safe:orbitNumber type="start"> or <safe:relativeOrbitNumber type="start">
    :param is_ew: True if EW data, False if IW data
    :return: Burst ID
    """
    mid_burst_datetime = datetime.fromisoformat(mid_burst_time)
    anx_datetime = datetime.fromisoformat(anx_time)

    time_distance = calc_timedistance(mid_burst_datetime, anx_datetime, orbit_number)
    burst_id = calc_burstid_from_timedist(time_distance, is_ew)
    # FIXME burst_id is off by one
    return int(burst_id) + 1


def update_burst_dataframe(df, zipname, swath, polarization):
    """
    The function to update the dataframes
    Parameters:
        zipname: the zip file which contains the satellite data
        swath: the swath of the slc file to extract bursts info from
        polarization: the polarization of the slc file to extract bursts info from
    """

    zf = zipfile.ZipFile(zipname, 'r')
    xmlpath = os.path.join('*SAFE', 'annotation', 's1[ab]-iw{}-slc-{}*xml'.format(swath, polarization))
    match = fnmatch.filter(zf.namelist(), xmlpath)
    xmlstr = zf.read(match[0])
    annotation_path = match[0]
    xml_root = ET.fromstring(xmlstr)
    # Burst interval
    burst_interval = 2.758277

    ascNodeTime = getxmlvalue(xml_root, "imageAnnotation/imageInformation/ascendingNodeTime")
    numBursts = getxmlattr(xml_root, 'swathTiming/burstList', 'count')
    burstList = getxmlelement(xml_root, 'swathTiming/burstList')
    passtype = getxmlvalue(xml_root, 'generalAnnotation/productInformation/pass')
    orbitNumber = int(getxmlvalue(xml_root, 'adsHeader/absoluteOrbitNumber'))
    # relative orbit number
    # link: https://forum.step.esa.int/t/sentinel-1-relative-orbit-from-filename/7042/20
    if os.path.basename(zipname).lower().startswith('s1a'):
        trackNumber = (orbitNumber - 73) % 175 + 1
    else:
        trackNumber = (orbitNumber - 27) % 175 + 1
    lineperburst = int(getxmlvalue(xml_root, 'swathTiming/linesPerBurst'))
    sampleperburst = int(getxmlvalue(xml_root, 'swathTiming/samplesPerBurst'))
    geocords, tiff_path = getCoordinates(zipname, swath, polarization)
    for index, burst in enumerate(list(burstList)):
        sensingStart = burst.find('azimuthTime').text
        date = read_time(sensingStart).strftime("%Y%m%dT%H%M%S")
        byte_offset = int(burst.find('byteOffset').text)
        dt = read_time(sensingStart) - read_time(ascNodeTime)
        time_info = int((dt.seconds + dt.microseconds / 1e6) / burst_interval)
        relative_burst_id = calculate_relative_burst_id(sensingStart, ascNodeTime, trackNumber, is_ew=False)
        absolute_burst_id = f'S1_SLC_{date}_{polarization.upper()}_{relative_burst_id}_IW{swath}'
        thisBurstCoords, xc, yc = burstCoords(geocords, lineperburst, index)

        df = pd.concat([df, pd.DataFrame.from_records([{'absoluteID': absolute_burst_id,
                                                        'relativeID': relative_burst_id,
                                                        'swath': swath,
                                                        'polarization': polarization,
                                                        'date': date,
                                                        'pass_direction': passtype,
                                                        'annotation': annotation_path,
                                                        'measurement': tiff_path,
                                                        'longitude': xc,
                                                        'latitude': yc,
                                                        'geometry': thisBurstCoords.wkt,
                                                        'lines': lineperburst,
                                                        'samples': sampleperburst,
                                                        'byte_offset': byte_offset,
                                                        }])])
    lengths = df['byte_offset'].rolling(2).apply(lambda x: x.iloc[1] - x.iloc[0]).iloc[1:]
    length = lengths.drop_duplicates()
    if length.shape[0] > 1:
        raise ValueError('Bursts have differing byte lengths')
    df['byte_length'] = int(length.iloc[0])

    zf.close()
    return df.drop_duplicates().reset_index(drop=True)


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