import base64
import functions_framework
import json
from google.cloud import pubsub_v1

import vertexai
from vertexai.generative_models import GenerativeModel
vertexai.init(project='midterm-440408', location='us-central1')

model = GenerativeModel("gemini-1.5-flash-002") 
prompt = """
I will attach a list of email bodies below, please extract the following information for each of them and provide them in the format below:

title#Name of the Company that the user applied for 
position#Position that was applied for
deadline#Any upcoming deadlines mentioned in the email, return it in the format YYYY-MM-DD. If not mentioned, return the last date of the year.
date_of_application#Date that the application was sent. If not present in the email body, return date today's date in YYYY-MM-DD format
notes#Summarize in a 4-7 words the update status of this application
status#Each email will be one of - 
    Applied - If this is just an acknowledgement email
    OA - If an online assessment or a test has been scheduled
    Interview - If an interview has been scheduled
    Offer - If there was an offer made (can be an acceptance)
    Rejection - If the applicant was rejected.

return all this fields as a JSON object of the following form:
{
    title: ...,
    position: ...,
    deadline: ...,
    date_of_application: ...,
    notes: ..., 
    status: ...
}
Here are the emails, each new email body starts after a "-----", return a list of all these json objects for each email body:
"""

psub_client = pubsub_v1.PublisherClient()
    
topic_path = psub_client.topic_path('midterm-440408','processed-emails')

def publish_message(messages):
    try:
        message_data = json.dumps(messages).encode("utf-8")
        future = psub_client.publish(topic_path, data=message_data)
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")


def call_llm(msg):
    # call llm with email content...

    a = model.generate_content(prompt + '-----'.join(msg))

    return json.loads(a.text.strip("```json\n").strip('\n```\n'))

# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    
    messages = json.loads(base64.b64decode(cloud_event.data["message"]["data"]))

    responses = call_llm([ msg['content'] for msg in messages ])

    print(responses)

    for idx in range(len(messages)):
        msg = messages[idx]
        resp = responses[idx]
        f = {
            "email": msg['email'],
            "title": resp['title'],
            "position": resp['position'],
            "status": resp['status'],
            "notes": resp['notes'],
            "deadline": resp['deadline'],
            "data_of_application": resp['date_of_application']
        }
        publish_message(f)