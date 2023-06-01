import os
import json
from random import randint
from time import sleep
import requests
from requests.auth import HTTPBasicAuth
import traceback

def main(request):

    # get the information passed from SWEEP workflow manager to this task instance
    fargs = request.get_json(silent=True)

    pred_outs = fargs['predecessor_outputs']
    static_input = fargs['static_input']
    start_datetime = static_input['start_datetime']
    end_datetime = static_input['end_datetime']

    task_feature = pred_outs['1']['feature']
    task_geometry = task_feature['geometry']['coordinates']
    feature_name = task_feature['properties']['name']

    print(f"Working on site {feature_name}")

    # Geometry: the geo json geometry object we got from geojson.io
    geo_json_geometry = {
        "type": "Polygon",
        "coordinates": task_geometry
    }

    # filter for items the overlap with our chosen geometry
    geometry_filter = {
        "type": "GeometryFilter",
        "field_name": "geometry",
        "config": geo_json_geometry
    }

    # filter for images acquired in a certain date range
    date_range_filter = {
        "type": "DateRangeFilter",
        "field_name": "acquired",
        "config": {
            "gte": start_datetime,
            "lt": end_datetime
        }
    }

    # placeholder for cloud cover filter (e.g. remove images which are more than 50% clouds)
    # currently not implemented
    cloud_cover_filter = {
        "type": "RangeFilter",
        "field_name": "cloud_cover",
        "config": {
        }
    }

    # Combine filters
    plot_1 = {
        "type": "AndFilter",
        "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }

    # Create search API request object
    search_endpoint_request = {
        "item_types": ["PSScene"],
        "filter": plot_1
    }

    # Perform the search
    result_metadata = requests.post(
        'https://api.planet.com/data/v1/quick-search',
        auth=HTTPBasicAuth(os.environ['PL_API_KEY'], ''),
        json=search_endpoint_request)

    if not result_metadata.status_code == requests.codes.ok:
        print(f"search request failed: {result_metadata.text}")

    # Throw exception if the search fails to trigger task retry by SWEEP
    if not result_metadata.ok:
        raise Exception(result_metadata.content)

    jstl = json.loads(result_metadata.text)

    # Get the image ids
    image_ids = [feature['id'] for feature in jstl['features']]
    count = len(image_ids)

    print(f"Found {count} matched items for the site")

    # Order the items

    # define products part of order
    single_product = [
        {
            "item_ids": image_ids,
            "item_type": "PSScene",
            "product_bundle": "analytic_sr_udm2"
        }
    ]

    clip_aoi_to = {
        "type": "Polygon",
        "coordinates": task_geometry
    }

    # define the clip tool
    clip = {
        "clip": {
            "aoi": clip_aoi_to
        }
    }

    # harmonize
    harmonizeclip = {
        "harmonize": {
            "target_sensor": "Sentinel-2"
        }
    }

    # create an order request with the clipping tool
    request_clip = {
        "name": feature_name,
        "products": single_product,
        "order_type": "partial",
        "tools": [clip, harmonizeclip]
    }

    print(f"Requesting order for {request_clip}")

    try:
        # Wait a random amount of time
        sleep(randint(50, 90))

        orders_url = 'https://api.planet.com/compute/ops/orders/v2'

        # Query the Planet API
        auth = HTTPBasicAuth(os.getenv('PL_API_KEY'), '')
        headers = {'content-type': 'application/json'}
        response = requests.post(orders_url,
                                 data=json.dumps(request_clip),
                                 auth=auth, headers=headers)
    except:
        print(traceback.format_exc())

    print(f"Order returned: {response}")

    order_id = ""
    order_url = ""

    if not response.ok:
        print(f"Order failed for task: {feature_name}")
    else:
        order_id = response.json()['id']
        print(f"Order placed successfully for task: {feature_name}, order number is: {order_id}")
        order_url = orders_url + '/' + order_id

    out_dict = {"order_id": order_id,
                "order_url": order_url,
                "item": [{"count": count}],
                "image_ids_ordered": image_ids,
                "geometry": task_geometry,
                "name": feature_name,
                "start_time": start_datetime,
                "end_time": end_datetime}

    return out_dict
