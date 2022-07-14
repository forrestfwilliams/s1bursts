import json
import os
import datetime
import xml
import xml.etree.ElementTree as ET
import zipfile
import fnmatch
import pandas as pd
import numpy as np
import geopandas as gpd
from shapely.geometry import Polygon
from datetime import datetime, timedelta

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
    poly = Polygon(zip(X, Y))
    xc, yc = poly.centroid.xy
    return poly, xc[0], yc[0]


# These formulas are from the abovementioned PDF, section 9.25
def calc_burstid_from_timedist(timedist: timedelta, is_ew: bool = False) -> int:
    prelen = PREAMBLE_LENGTH_EW if is_ew else PREAMBLE_LENGTH_IW
    beamtime = BEAM_CYCLE_TIME_EW if is_ew else BEAM_CYCLE_TIME_IW
    burstid = np.floor((timedist - prelen) / beamtime) + 1
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

    return calc_burstid_from_timedist(time_distance, is_ew)


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
        byte_offset = int(burst.find('byteOffset').text)
        dt = read_time(sensingStart) - read_time(ascNodeTime)
        time_info = int((dt.seconds + dt.microseconds / 1e6) / burst_interval)
        burstID = "t" + str(trackNumber) + "s" + str(swath) + "b" + str(time_info)
        relative_burst_id = calculate_relative_burst_id(sensingStart, ascNodeTime, orbitNumber, is_ew=False)
        thisBurstCoords, xc, yc = burstCoords(geocords, lineperburst, index)

        df = pd.concat([df, pd.DataFrame.from_records([{'burst_ID': burstID,
                                                        'relativeID': relative_burst_id,
                                                        'swath': swath,
                                                        'polarization': polarization,
                                                        'date': read_time(
                                                            sensingStart).strftime(
                                                            "%Y%m%dT%H%M%S"),
                                                        'pass_direction': passtype,
                                                        'annotation': annotation_path,
                                                        'measurement': tiff_path,
                                                        'longitude': xc,
                                                        'latitude': yc,
                                                        'geometry': thisBurstCoords.wkt,
                                                        'byte_offset': byte_offset,
                                                        'lines': lineperburst,
                                                        'samples': sampleperburst,
                                                        }])])

    zf.close()
    return df.drop_duplicates().reset_index()


def generate_stac_collection():
    pass
    return None