import base64
import functions_framework

from concurrent.futures import TimeoutError

import joblib
from google.cloud import storage

from google.cloud import pubsub_v1

from dill import load

import json

storage_client = storage.Client()

subscriber = pubsub_v1.SubscriberClient()

def publish_message(messages):
    psub_client = pubsub_v1.PublisherClient()
    
    topic_path = psub_client.topic_path('midterm-440408','rel-mail')

    try:
        message_data = messages.encode("utf-8")
        future = psub_client.publish(topic_path, data=message_data)
        
        print(f"Published message to rel-mail. ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")

def load_CRFObject():
    object_path = '/tmp/CRFObject.pkl'

    bucket = storage_client.bucket('talk2data-bkt')

    blob = bucket.blob('CRFO.pkl')

    blob_ = blob.download_to_filename(object_path)

    with open(object_path, 'rb') as f:
        crf_object = load(f)

    return crf_object

@functions_framework.http
def hello_main(cloud_event):
    crf_object = load_CRFObject()

    subscription_path = subscriber.subscription_path('midterm-440408', 'aptrack-sub')
    
    message_list = []

    def callback(message):
        print(f"Received message.")
        b = json.loads(message.data.decode('utf-8'))
        message_list.append(b)
        message.ack()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    with subscriber:
        try:
            streaming_pull_future.result(timeout=5)
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

    text_data = [email['content'] for email in message_list]

    classified_results = crf_object.run(text_data)

    class_final = []

    for i in range(len(message_list)):
        if classified_results[i] != 'irrelevant':
            e_mail = message_list[i]['email']
            class_final.append({'email': e_mail, 'content': message_list[i]['content'], 'status': classified_results[i]})

    publish_message(json.dumps(class_final))

    return {'h': class_final}
