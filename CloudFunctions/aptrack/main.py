import base64
import functions_framework

import joblib
from google.cloud import storage

from google.cloud import pubsub_v1

from dill import load

import json

storage_client = storage.Client()

def publish_message(messages):
    psub_client = pubsub_v1.PublisherClient()
    
    topic_path = psub_client.topic_path('probable-surge-441900-q0', 'ProcessedEmailQueue')

    try:
        message_data = messages.encode("utf-8")
        future = psub_client.publish(topic_path, data=message_data)
        
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_CRFObject():
    object_path = '/tmp/CRFObject.pkl'

    bucket = storage_client.bucket('BKT_NAME')

    blob = bucket.blob('CRFO.pkl')

    blob_ = blob.download_to_filename(object_path)

    with open(object_path, 'rb') as f:
        crf_object = load(f)

    return crf_object

@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    a = base64.b64decode(cloud_event.data["message"]["data"])

    emails = json.loads(a.decode('utf-8').replace("'", '"'))

    text_data = [email['content'] for email in emails]

    email_mdata = [(email['sender'], email['subject']) for email in emails]

    # publish_message(json.dumps(list(zip(email_mdata, predictions))))

    crf_object = load_CRFObject()

    a = json.dumps(list(zip(email_mdata, crf_object.run(text_data))))

    print(a)

    publish_message(a)
