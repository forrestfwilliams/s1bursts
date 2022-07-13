import json
import os
import datetime
import xml
import xml.etree.ElementTree as ET
import zipfile
import fnmatch
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon


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

    dt = datetime.datetime.strptime(input_str, fmt)
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


# @staticmethod
# def get_relative_burst_id():

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
        thisBurstCoords, xc, yc = burstCoords(geocords, lineperburst, index)
        # check if self.df has this dt for this track. If not append it

        burst_query = self.df.query("burst_ID=='{}'".format(burstID))
        if burst_query.empty:
            print("adding {} to the dataframe".format(burstID))

            self.df = pd.concat([self.df, pd.DataFrame.from_records([{'burst_ID': burstID,
                                                                      'pass_direction': passtype,
                                                                      'longitude': xc,
                                                                      'latitude': yc,
                                                                      'geometry': thisBurstCoords.wkt
                                                                      }])])

        else:
            print('The Unique ID {} already exists.'.format(burstID))

        df = pd.concat([df, pd.DataFrame.from_records([{'burst_ID': burstID,
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
    return df
