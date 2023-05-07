import base64
import hashlib

from csv2bufr import BUFRMessage
from dagster import asset, get_dagster_logger
from datetime import datetime as dt
import json
import paho.mqtt.publish as publish
import requests
import ssl
import uuid


@asset
def get_data():
    # get logger
    logger = get_dagster_logger()
    # set data URL, in this example we are looking at the 'livedata' for
    # Phillip Goldston International Airport
    data_url_ = "http://surface.opencdms.org/api/livedata/9958303/?format=json"
    # Get the get
    data = requests.get(data_url_).json()
    # Extract the required information
    latest = data.get('latest', None)
    if latest is None:
        logger.info("No data found")
        return None

    # in this example case the WMO number is missing, for illustrative
    # purposes we use the wsi for the previous station at this site.
    data['station']['wigos'] = '0-20000-0-78583'

    timestamp_string = data.get('latest_last_update', None)
    if timestamp_string is None:
        logger.info("Missing timestamp")
        return None

    date_format = "%Y-%m-%dT%H:%M:%SZ"
    timestamp = dt.strptime(timestamp_string, date_format)

    wsi = (data['station']['wigos']).split('-')

    output = {
        'wsi_series': wsi[0],
        'wsi_issuer': wsi[1],
        'wsi_issue_number': wsi[2],
        'wsi_local': wsi[3],
        'station_name': data['station']['name'],
        'station_type': 0, # AWS,
        'year': timestamp.year,
        'month': timestamp.month,
        'day': timestamp.day,
        'hour': timestamp.hour,
        'minute': timestamp.minute,
        'latitude': float(data['station']['latitude']),
        'longitude': float(data['station']['longitude']),
        'elevation': float(data['station']['elevation']),
        'barometer_height': float(data['station']['elevation']) + 2.0,
        'thermometer_height': 2.0,
        'air_temperature': None,
        'wet_bulb_temperature': None,
        'dew_point_temperature': None,
        'pressure': None,
        'anemometer_height': None,
        'anemometer_type': None,
        'wind_speed_significance': 2,
        'wind_direction': None,
        'wind_speed': None,
        'pressure_reduced_to_mean_sea_level': None,
        'timestamp': timestamp_string,
        'wsi': data['station']['wigos']
    }
    for observation in latest:
        if observation['variable']['symbol'] == "TEMP":
            output['air_temperature'] = float(observation['value'])
        if observation['variable']['symbol'] == "TEMPWB":
            output['wet_bulb_temperature'] = float(observation['value'])
        if observation['variable']['symbol'] == "TDEWPNT":
            output['dew_point_temperature'] = float(observation['value'])
        if observation['variable']['symbol'] == "PRESSTN":
            output['pressure'] = float(observation['value'])
        if observation['variable']['symbol'] == "WNDDAVG":
            output['wind_direction'] = float(observation['value'])
        if observation['variable']['symbol'] == "WNDSPAVG":
            output['wind_speed'] = float(observation['value'])

    return output

@asset
def convert_to_bufr(get_data):
    logger = get_dagster_logger()
    bufr_mapping = {
        "inputDelayedDescriptorReplicationFactor": [],
        "inputShortDelayedDescriptorReplicationFactor": [],
        "inputExtendedDelayedDescriptorReplicationFactor": [],
        "wigos_station_identifier": "data:wsi",
        "number_header_rows": 1,
        "column_names_row": 1,
        "QUOTING": "QUOTE_NONE",
        "header": [
            {"eccodes_key": "edition", "value": "const:4"},
            {"eccodes_key": "masterTableNumber", "value": "const:0"},
            {"eccodes_key": "updateSequenceNumber", "value": "const:0"},
            {"eccodes_key": "dataCategory", "value": "const:0"},
            {"eccodes_key": "internationalDataSubCategory", "value": "const:6"},
            {"eccodes_key": "masterTablesVersionNumber", "value": "const:38"},
            {"eccodes_key": "typicalYear", "value": "data:year"},
            {"eccodes_key": "typicalMonth", "value": "data:month"},
            {"eccodes_key": "typicalDay", "value": "data:day"},
            {"eccodes_key": "typicalHour", "value": "data:hour"},
            {"eccodes_key": "typicalMinute", "value": "data:minute"},
            {"eccodes_key": "numberOfSubsets", "value": "const:1"},
            {"eccodes_key": "observedData", "value": "const:1"},
            {"eccodes_key": "compressedData", "value": "const:0"},
            {"eccodes_key": "unexpandedDescriptors", "value": "array: 301150, 307082"}
        ],
        "data": [
            {"eccodes_key": "#1#wigosIdentifierSeries", "value": "data:wsi_series"},
            {"eccodes_key": "#1#wigosIssuerOfIdentifier", "value": "data:wsi_issuer"},
            {"eccodes_key": "#1#wigosIssueNumber", "value": "data:wsi_issue_number"},
            {"eccodes_key": "#1#wigosLocalIdentifierCharacter", "value": "data:wsi_local"},
            {"eccodes_key": "#1#stationOrSiteName", "value": "data:station_name"},
            {"eccodes_key": "#1#stationType", "value": "data:station_type"},
            {"eccodes_key": "#1#year", "value": "data:year", "valid_min": "const:2000", "valid_max": "const:2100"},
            {"eccodes_key": "#1#month", "value": "data:month", "valid_min": "const:1", "valid_max": "const:12"},
            {"eccodes_key": "#1#day", "value": "data:day", "valid_min": "const:1", "valid_max": "const:31"},
            {"eccodes_key": "#1#hour", "value": "data:hour", "valid_min": "const:0", "valid_max": "const:23"},
            {"eccodes_key": "#1#minute", "value": "data:minute", "valid_min": "const:0", "valid_max": "const:59"},
            {"eccodes_key": "#1#latitude", "value": "data:latitude", "valid_min": "const:-90", "valid_max": "const:90"},
            {"eccodes_key": "#1#longitude", "value": "data:longitude", "valid_min": "const:-180", "valid_max": "const:180"},
            {"eccodes_key": "#1#heightOfStationGroundAboveMeanSeaLevel", "value": "data:elevation"},
            {"eccodes_key": "#1#heightOfBarometerAboveMeanSeaLevel", "value": "data:barometer_height"},
            {"eccodes_key": "#1#nonCoordinatePressure", "value": "data:pressure", "scale": "const:1", "offset": "const:0"},
            {"eccodes_key": "#1#pressureReducedToMeanSeaLevel", "value": "data:pressure_reduced_to_mean_sea_level"},  # missing
            {"eccodes_key": "#1#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "data:thermometer_height"},
            {"eccodes_key": "#1#airTemperature", "value": "data:air_temperature", "scale": "const:0", "offset": "const:273.15"},
            {"eccodes_key": "#1#dewpointTemperature", "value": "data:dew_point_temperature", "scale": "const:0", "offset": "const:273.15"},
            {"eccodes_key": "#7#heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform", "value": "data:anemometer_height"},
            {"eccodes_key": "#1#instrumentationForWindMeasurement", "value": "data:anemometer_type"},
            {"eccodes_key": "#1#timeSignificance", "value": "data:wind_speed_significance"},
            {"eccodes_key": "#10#timePeriod", "value": "const:-10"},
            {"eccodes_key": "#1#windDirection", "value": "data:wind_direction"},
            {"eccodes_key": "#1#windSpeed", "value": "data:wind_speed"}
        ]
    }

    output = {}
    try:
        logger.info(json.dumps(get_data))
        logger.info(json.dumps(bufr_mapping))
        message = BUFRMessage([301150, 307082], [], [], [], 38)
        message.parse( get_data, bufr_mapping )
        result = base64.b64encode(message.as_bufr()).decode("utf-8")
        hash = hashlib.sha256(message.as_bufr()).hexdigest()
        hash_method = "sha256"
        output = {
            "bufr": {
                'encoding': 'base64',
                'value': result,
                'size': len(result)
            },
            "_meta": {
                'longitude':get_data['longitude'],
                'latitude': get_data['latitude'],
                'dataid': asset,
                'datetime': get_data['timestamp'],
                'hash_method': hash_method,
                'hash_value': hash,
                'wsi': get_data['wsi']
            }
        }
        return output

    except Exception as e:
        logger.error(f"Error converting to BUFR: {e}")

    return None


@asset
def publish_message(convert_to_bufr):
    logger = get_dagster_logger()
    #mqtt connection details, move to env file
    topic = "origin/a/wis2/blz/nms_db/data/core/weather/surface-based-observations/synop"

    tls = {
        'ca_certs': cert_file,
        'tls_version': ssl.PROTOCOL_TLSv1_2
    }
    data_id = f"{topic}/{convert_to_bufr['_meta']['hash_value']}"
    # now build the message to publish
    msg = {
        'id': str(uuid.uuid4()),
        'type': 'Feature',
        'version': 'v04',
        'geometry': {
            'type': 'Point',
            'coordinates': [convert_to_bufr['_meta']['longitude'], convert_to_bufr['_meta']['latitude']]
        },
        'properties': {
            'data_id': data_id,
            'datetime': convert_to_bufr['_meta']['datetime'],
            'pubtime': dt.now().isoformat(),
            'integrity': {
                'method': convert_to_bufr['_meta']['hash_method'],
                'value': convert_to_bufr['_meta']['hash_value'],
            },
            'wigos_station_identifier': convert_to_bufr['_meta']['wsi'],
            'content': convert_to_bufr['bufr']
        },
        'links': [
            {
                'rel': 'canonical',
                'href': f"www.example.com/blz/api/bufr/{convert_to_bufr['_meta']['hash_value']}",
                'type': 'application/x-bufr'
            }
        ]
    }
    # publish single
    try:
        logger.info(f"Publishing to {topic}")
        payload = json.dumps(msg)
        publish.single(topic, payload=json.dumps(msg), qos=0,
                       retain=False, hostname=broker, port=port, auth=auth,
                       tls=tls)
        payload_size = len(payload)
        logger.info( f"Published {payload_size} bytes")
        return 0
    except Exception as e:
        logger.error(e)
        return -1


