import datetime
import json

import requests
from shapely.geometry import Polygon

import s1bursts


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


def build_attr(name, value):
    attr = {
        'Name': name,
    }
    if isinstance(value, list):
        attr['Values'] = [str(v) for v in value]
    else:
        attr['Values'] = [str(value)]
    return attr


def generate_umm(slc, burst):
    now = datetime.datetime.now(tz=datetime.timezone.utc).isoformat()

    swath = burst.absolute_burst_id.split('_')[-1]
    granule_ur = f'S1_SLC_{burst.sensing_start.split(".")[0].replace("-", "").replace(":", "")}_{burst.polarization.upper()}_{burst.relative_burst_id:06}_{swath}'

    points = BURST_MAP[f'{burst.relative_burst_id:06}_{swath}']
    polygon = Polygon([[point['Longitude'], point['Latitude']] for point in points])

    slc_attribute_names = [
        'ASCENDING_DESCENDING',
        'BEAM_MODE',
        'BEAM_MODE_TYPE',
        'PATH_NUMBER',
        'SV_POSITION_POST',
        'SV_POSITION_PRE',
        'SV_VELOCITY_POST',
        'SV_VELOCITY_PRE',
        'LOOK_DIRECTION',
    ]
    additional_attributes = [attr for attr in slc['AdditionalAttributes'] if attr['Name'] in slc_attribute_names]

    additional_attributes.append(build_attr('PROCESSING_TYPE', 'S1_SLC_BURSTS'))
    additional_attributes.append(build_attr('GROUP_ID', granule_ur))
    additional_attributes.append(build_attr('POLARIZATION', burst.polarization.upper()))
    additional_attributes.append(build_attr('RELATIVE_BURST_ID', burst.relative_burst_id))
    additional_attributes.append(build_attr('SWATH', swath))
    additional_attributes.append(build_attr('ASC_NODE_TIME', burst.sensing_start))
    additional_attributes.append(build_attr('CENTER_LON', polygon.centroid.x))
    additional_attributes.append(build_attr('CENTER_LAT', polygon.centroid.y))

    additional_attributes.append(build_attr('ANNOTATION_PATH', burst.annotation_path))
    additional_attributes.append(build_attr('AZIMUTH_FRAME_RATE', str(burst.azimuth_frame_rate)))
    additional_attributes.append(build_attr('AZIMUTH_STEER_RATE', burst.azimuth_steer_rate))
    additional_attributes.append(build_attr('AZIMUTH_TIME_INTERVAL', burst.azimuth_time_interval))
    additional_attributes.append(build_attr('BURST_ANX', burst.burst_anx))
    additional_attributes.append(build_attr('BURST_ANX_DELTA', burst.burst_anx_delta))
    additional_attributes.append(build_attr('BURST_INDEX', burst.burst_index))
    additional_attributes.append(build_attr('BYTE_LENGTH', burst.byte_length))
    additional_attributes.append(build_attr('BYTE_OFFSET', burst.byte_offset))
    additional_attributes.append(build_attr('DOPPLER', str(burst.doppler)))
    additional_attributes.append(build_attr('FIRST_VALID_LINE', burst.first_valid_line))
    additional_attributes.append(build_attr('FIRST_VALID_SAMPLE', burst.first_valid_sample))
    additional_attributes.append(build_attr('IW2_MID_RANGE', burst.iw2_mid_range))
    additional_attributes.append(build_attr('LAST_VALID_LINE', burst.last_valid_line))
    additional_attributes.append(build_attr('LAST_VALID_SAMPLE', burst.last_valid_sample))
    additional_attributes.append(build_attr('LINES', burst.lines))
    additional_attributes.append(build_attr('MEASUREMENT_PATH', burst.measurement_path))
    additional_attributes.append(build_attr('OPERA_ID', burst.opera_id))
    additional_attributes.append(build_attr('PRF_RAW_DATA', burst.prf_raw_data))
    additional_attributes.append(build_attr('RADAR_CENTER_FREQUENCY', burst.radar_center_frequency))
    additional_attributes.append(build_attr('RANGE_BANDWIDTH', burst.range_bandwidth))
    additional_attributes.append(build_attr('RANGE_CHIRP_RATE', burst.range_chirp_rate))
    additional_attributes.append(build_attr('RANGE_PIXEL_SPACING', burst.range_pixel_spacing))
    additional_attributes.append(build_attr('RANGE_SAMPLING_RATE', burst.range_sampling_rate))
    additional_attributes.append(build_attr('RANGE_WINDOW_COEFFICIENT', burst.range_window_coefficient))
    additional_attributes.append(build_attr('RANGE_WINDOW_TYPE', burst.range_window_type))
    additional_attributes.append(build_attr('RANK', burst.rank))
    additional_attributes.append(build_attr('SAFE_NAME', burst.safe_name))
    additional_attributes.append(build_attr('SAFE_URL', burst.safe_url))
    additional_attributes.append(build_attr('SAMPLES', burst.samples))
    additional_attributes.append(build_attr('SLANT_RANGE_TIME', burst.slant_range_time))
    additional_attributes.append(build_attr('SLC_START_ANX', burst.slc_start_anx))
    additional_attributes.append(build_attr('STARTING_RANGE', burst.starting_range))
    additional_attributes.append(build_attr('SWATH_INDEX', burst.swath_index))
    additional_attributes.append(build_attr('WAVELENGTH', burst.wavelength))

    umm = {
        'TemporalExtent': {
            'RangeDateTime': {
                'BeginningDateTime': f'{burst.sensing_start}+00:00',
                'EndingDateTime': f'{burst.sensing_stop}+00:00',
            },
        },
        'OrbitCalculatedSpatialDomains': slc['OrbitCalculatedSpatialDomains'],
        'GranuleUR': granule_ur,
        'AdditionalAttributes': additional_attributes,
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
    urls = [slc['RelatedUrls'][0]['URL'] for slc in cmr_slcs]
    burst_list = s1bursts.get_burst_metadata(urls, threads=20)

    for burst in burst_list:
        slc = [slc for slc in cmr_slcs if f"{slc['DataGranule']['Identifiers'][0]['Identifier']}.SAFE" == burst.safe_name][0]
        umm = generate_umm(slc, burst)
        print(umm['GranuleUR'])
        with open(f'umm/{umm["GranuleUR"]}.json', 'w') as f:
            json.dump(umm, f, indent=2)
