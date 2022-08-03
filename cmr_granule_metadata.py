import datetime
import json

import requests
from shapely.geometry import Polygon

import bursts


with open('burst_locations_by_id.json') as f:
    BURST_MAP = json.load(f)


def cmr_query(params):
    session = requests.Session()
    cmr_url = 'https://cmr.earthdata.nasa.gov/search/granules.umm_json'
    headers = {}
    products = []

    while True:
        response = session.get(cmr_url, params=params, headers=headers)
        response.raise_for_status()
        products.extend([item['umm'] for item in response.json()['items']])
        if 'CMR-Search-After' not in response.headers:
            break
        headers = {'CMR-Search-After': response.headers['CMR-Search-After']}

    return products


def get_galapagos_cmr_slcs():
    params = {
        'provider': 'ASF',
        'bounding_box': '-95,-4,-85,3',
        'short_name': [
            'SENTINEL-1A_SLC',
            'SENTINEL-1B_SLC',
        ],
        'attribute[]': 'string,BEAM_MODE,IW',
        'temporal': '2021-11-11T00:00:00Z,',
        'page_size': 2000,
    }
    return cmr_query(params)


def get_attr_values(name, attributes):
    for attr in attributes:
        if attr['Name'] == name:
            return attr['Values']


def generate_umm(slc, burst):
    now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
    swath = burst.absolute_burst_id.split('_')[-1]
    granule_ur = f'S1_SLC_{burst.sensing_start.split(".")[0].replace("-", "").replace(":", "")}_{burst.polarization.upper()}_{burst.relative_burst_id:06}_{swath}'
    points = BURST_MAP[f'{burst.relative_burst_id:06}_{swath}']
    polygon = Polygon([[point['Longitude'], point['Latitude']] for point in points])
    umm = {
        'TemporalExtent': {
            'RangeDateTime': {
                'BeginningDateTime': f'{burst.sensing_start}+00:00',
                'EndingDateTime': f'{burst.sensing_stop}+00:00',
            },
        },
        'OrbitCalculatedSpatialDomains': slc['OrbitCalculatedSpatialDomains'],
        'GranuleUR': granule_ur,
        'AdditionalAttributes': [
            {
                'Name': 'GROUP_ID',
                'Values': [
                    granule_ur,
                ],
            },
            {
                'Name': 'PROCESSING_TYPE',
                'Values': [
                    'S1_SLC_BURSTS',
                ],
            },
            {
                'Name': 'POLARIZATION',
                'Values': [
                    burst.polarization.upper(),
                ],
            },
            {
                'Name': 'BEAM_MODE',
                'Values': get_attr_values('BEAM_MODE', slc['AdditionalAttributes']),
            },
            {
                'Name': 'BEAM_MODE_TYPE',
                'Values': get_attr_values('BEAM_MODE_TYPE', slc['AdditionalAttributes']),
            },
            {
                'Name': 'ASCENDING_DESCENDING',
                'Values': get_attr_values('ASCENDING_DESCENDING', slc['AdditionalAttributes']),
            },
            {
                'Name': 'PATH_NUMBER',
                'Values': get_attr_values('PATH_NUMBER', slc['AdditionalAttributes']),
            },
            {
                'Name': 'RELATIVE_BURST_ID',
                'Values': [
                    str(burst.relative_burst_id),
                ],
            },
            {
                'Name': 'SWATH',
                'Values': [
                    swath,
                ],
            },
            {
                'Name': 'SV_POSITION_POST',
                'Values': get_attr_values('SV_POSITION_POST', slc['AdditionalAttributes']),
            },
            {
                'Name': 'SV_POSITION_PRE',
                'Values': get_attr_values('SV_POSITION_PRE', slc['AdditionalAttributes']),
            },
            {
                'Name': 'SV_VELOCITY_POST',
                'Values': get_attr_values('SV_VELOCITY_POST', slc['AdditionalAttributes']),
            },
            {
                'Name': 'SV_VELOCITY_PRE',
                'Values': get_attr_values('SV_VELOCITY_PRE', slc['AdditionalAttributes']),
            },
            {
                'Name': 'ASC_NODE_TIME',
                'Values': [
                    burst.sensing_start,
                ],
            },
            {
                'Name': 'CENTER_LON',
                'Values': [
                    str(polygon.centroid.x),
                ],
            },
            {
                'Name': 'CENTER_LAT',
                'Values': [
                    str(polygon.centroid.y),
                ],
            },
            {
                'Name': 'LOOK_DIRECTION',
                'Values': get_attr_values('LOOK_DIRECTION', slc['AdditionalAttributes']),
            },
        ],
        'SpatialExtent': {
            'HorizontalSpatialDomain': {
                'Geometry': {
                    'GPolygons': [
                        {
                            'Boundary': {
                                'Points': points,
                            },
                        },
                    ],
                },
            },
        },
        'ProviderDates': [
            {
                'Date': now,
                'Type': 'Insert',
            },
            {
                'Date': now,
                'Type': 'Update',
            },
        ],
        'CollectionReference': {
            'ShortName': 'S1_SLC_BURSTS',
            'Version': '1',
        },
        'RelatedUrls': [
            {
                'URL': f'https://asj-dev.s3.us-west-2.amazonaws.com/bursts/data/{granule_ur}.tiff',
                'Type': 'GET DATA',
            }
        ],
        'DataGranule': {
            'DayNightFlag': 'Unspecified',
            'Identifiers': [
                {
                    'Identifier': granule_ur,
                    'IdentifierType': 'ProducerGranuleId',
                },
            ],
            'ProductionDateTime': now,
            'ArchiveAndDistributionInformation': [
                {
                    'Name': f'{granule_ur}.tiff',
                    'SizeInBytes': 1,
                },
            ],
        },
        'Platforms': [
            {
                'ShortName': 'Sentinel-1A' if burst.platform == 'S1A' else 'Sentinel-1B',
                'Instruments': [
                    {
                        'ShortName': 'C-SAR',
                    },
                ],
            },
        ],
        'MetadataSpecification': {
            'URL': 'https://cdn.earthdata.nasa.gov/umm/granule/v1.6.4',
            'Name': 'UMM-G',
            'Version': '1.6.4',
        },
    }
    return umm


if __name__ == '__main__':
    cmr_slcs = get_galapagos_cmr_slcs()
    for slc in cmr_slcs:
        urls = [slc['RelatedUrls'][0]['URL'], ]
        burst_list = bursts.get_burst_metadata(urls, threads=20)
        for burst in burst_list:
            umm = generate_umm(slc, burst)
            print(umm['GranuleUR'])
            with open(f'umm/{umm["GranuleUR"]}.json', 'w') as f:
                json.dump(umm, f, indent=2)
        break
