from google.cloud import storage
import json
import os


def save_json_to_cloud(filename, dct, key_json, bucket_name):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_json
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(
       data=json.dumps(dct),
       content_type='application/json'
     )