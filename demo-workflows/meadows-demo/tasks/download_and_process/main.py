import requests
import pathlib
from requests.auth import HTTPBasicAuth
import csv
import boto3
import os
import uuid
import numpy as np
import rasterio

s3 = boto3.resource('s3',
                    aws_access_key_id=os.environ['ACCESS_KEY'],
                    aws_secret_access_key=os.environ['SECRET_KEY'])

def main(request):

    fargs = request.get_json(silent=True)
    pred_outs = fargs['predecessor_outputs']

    predecessor_task_output = pred_outs[list(pred_outs.keys())[0]]
    order_url = predecessor_task_output['order_url']

    site_id = predecessor_task_output['name']
    object_url = ""

    print("Working on site", site_id)
    print("Order url: ", order_url)

    PLANET_API_KEY = os.getenv('PL_API_KEY')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    FILEPATH = os.getenv('FILEPATH')

    # set up requests to work with api
    auth = HTTPBasicAuth(PLANET_API_KEY, '')

    overwrite = True

    r = requests.get(order_url, auth=auth)
    response = r.json()
    state = response['state']
    success_states = ['success', 'partial']

    if state == 'failed':
        print("order failed for  ", site_id)
    elif state in success_states:
        print("order succeeded for  ", site_id)
        results = response['_links']['results']
        results_urls = [r['location'] for r in results]
        results_names = [r['name'] for r in results]
        results_paths = [pathlib.Path(os.path.join('/tmp/data', n)) for n in results_names]
        print('{} items to download'.format(len(results_urls)))

        for url, name, path in zip(results_urls, results_names, results_paths):
            if overwrite or not path.exists():
                print('downloading {} to {}'.format(name, path))
                r = requests.get(url, allow_redirects=True)
                path.parent.mkdir(parents=True, exist_ok=True)
                open(path, 'wb').write(r.content)
            else:
                print('{} already exists, skipping {}'.format(path, name))

        downloaded_files = dict(zip(results_names, results_paths))
        img_files = [downloaded_files[d] for d in downloaded_files
                     if d.endswith('_3B_AnalyticMS_SR_clip.tif')]

        # Id of the file
        file_log = uuid.uuid4().hex

        header = ["Name", "Item_ID", "ndvi_mean", "ndvi_min", "ndvi_max", "red_mean", "red_min", "red_max", "blue_mean",
                  "blue_min", "blue_max", "green_mean", "green_min", "green_max", "nir_mean", "nir_min", "nir_max"]

        with open(FILEPATH + file_log + '_v2_planet_summary.csv', 'w') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(header)

        # Iterate through the tiff files and calculate ndvi and other metrics
        for filename_clip in img_files:
            with rasterio.open(filename_clip) as src:
                band_blue = src.read(1)
            with rasterio.open(filename_clip) as src:
                band_green = src.read(2)
            with rasterio.open(filename_clip) as src:
                band_red = src.read(3)
            with rasterio.open(filename_clip) as src:
                band_nir = src.read(4)

            # Upload the file
            clip_key = 'output/Planet/' + site_id + '/' + file_log + '/' + str(filename_clip.name)
            s3.meta.client.upload_file(str(filename_clip), S3_BUCKET_NAME, clip_key)
            print("Uploaded the clip", str(filename_clip))

            # Multiply the Digital Number (DN) values in each band by the TOA reflectance coefficients
            band_red = band_red
            band_green = band_green
            band_blue = band_blue
            band_nir = band_nir

            # Allow division by zero
            np.seterr(divide='ignore', invalid='ignore')

            # Calculate NDVI. This is the equation at the top of this guide expressed in code
            ndvi = (band_nir.astype(float) - band_red.astype(float)) / (band_nir + band_red)

            write = []
            name = site_id
            item_id = str(filename_clip.name)
            ndvi_mean = np.nanmean(ndvi)
            ndvi_min = np.nanmin(ndvi)
            ndvi_max = np.nanmax(ndvi)
            red_mean = np.nanmean(band_red)
            red_min = np.nanmin(band_red)
            red_max = np.nanmax(band_red)
            blue_mean = np.nanmean(band_blue)
            blue_min = np.nanmin(band_blue)
            blue_max = np.nanmax(band_blue)
            green_mean = np.nanmean(band_green)
            green_min = np.nanmin(band_green)
            green_max = np.nanmax(band_green)
            nir_mean = np.nanmean(band_nir)
            nir_min = np.nanmin(band_nir)
            nir_max = np.nanmax(band_nir)

            print(f"name: {name} item_id: {item_id} ndvi mean: {ndvi_mean}")
            write.append(name)
            write.append(item_id)
            write.append(ndvi_mean)
            write.append(ndvi_min)
            write.append(ndvi_max)
            write.append(red_mean)
            write.append(red_min)
            write.append(red_max)
            write.append(blue_mean)
            write.append(blue_min)
            write.append(blue_max)
            write.append(green_mean)
            write.append(green_min)
            write.append(green_max)
            write.append(nir_mean)
            write.append(nir_min)
            write.append(nir_max)

            with open(FILEPATH + file_log + '_v2_planet_summary.csv', 'a') as writeFile:
                writer = csv.writer(writeFile)
                writer.writerow(write)

        # Upload
        key = 'output/Planet/' + file_log + '_' + site_id + '_v2_planet_summary.csv'
        s3.meta.client.upload_file(FILEPATH + file_log + '_v2_planet_summary.csv', S3_BUCKET_NAME, key)

        object_acl = s3.ObjectAcl(S3_BUCKET_NAME, key)
        response = object_acl.put(ACL='public-read')
        print("Set ACL: ", response)

        object_url = "https://{0}.s3.us-west-2.amazonaws.com/{1}".format(S3_BUCKET_NAME, key)
        print("csv file at ", object_url)

    out_dict = {"order_url": order_url, "name": site_id, "s3url": object_url}

    return out_dict
