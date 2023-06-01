import json
import csv
import boto3
import os
import uuid

s3 = boto3.resource('s3', aws_access_key_id=os.environ['ACCESS_KEY'],
                    aws_secret_access_key=os.environ['SECRET_KEY'])

def main(request):
    FILEPATH = os.environ['FILEPATH']
    S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']

    fargs = request.get_json(silent=True)
    pred_outs = fargs['predecessor_outputs']

    # Id of the file
    file_log = uuid.uuid4().hex
    outfile_name = file_log + '_summary.csv'

    # Write the csv file
    header = ["Name", "order_url", "output_url"]
    with open(FILEPATH + outfile_name, 'w') as writeFile:
        writer = csv.writer(writeFile)
        writer.writerow(header)

    # Get list of dictionary entries
    satellite_output = [value for key, value in pred_outs.items()]
    for item in satellite_output:
        write = []
        name = item['name']
        order_url = item['order_url']
        s3url = item['s3url']
        print(f"name: {name} order output: {order_url} s3 output: {s3url}")
        write.append(name)
        write.append(order_url)
        write.append(s3url)

        with open(FILEPATH + outfile_name, 'a') as writeFile:
            writer = csv.writer(writeFile)
            writer.writerow(write)

    # Upload to S3
    s3.meta.client.upload_file(FILEPATH + outfile_name, S3_BUCKET_NAME,
                               'output/Planet/' + outfile_name)
    print("Uploaded")
    object_acl = s3.ObjectAcl(S3_BUCKET_NAME, 'output/Planet/' + outfile_name)
    response = object_acl.put(ACL='public-read')

    print("End workflow - file at ", FILEPATH + outfile_name)

    out_dict = {
        'statusCode': 200,
        'workflow_output': 'output/' + outfile_name,
        'body': json.dumps('End of workflow.')
    }

    return out_dict
