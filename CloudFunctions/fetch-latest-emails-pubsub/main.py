import base64
import functions_framework
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import firestore
from google.cloud import pubsub_v1
# import vertexai
# from vertexai.generative_models import GenerativeModel

# Initialize Firestore client
db = firestore.Client()

# Initialize Vertex AI client
# ertexai.init(project='midterm-440408', location='us-central1')

@functions_framework.cloud_event
def gmail_webhook(cloud_event):
    pubsub_message = base64.b64decode(cloud_event.data['message']['data'])
    data = json.loads(pubsub_message)
    user_email = data.get('emailAddress')
    new_history_id = data.get('historyId')

    # Get user data from Firestore
    user_ref = db.collection('users').document(user_email)
    user_doc = user_ref.get()
    if not user_doc.exists:
        print(f"User {user_email} not found in Firestore")
        return

    user_data = user_doc.to_dict()
    credentials = Credentials(
        token=user_data['token'],
        refresh_token=user_data['refresh_token'],
        token_uri=user_data['token_uri'],
        client_id=user_data['client_id'],
        client_secret=user_data['client_secret'],
        scopes=user_data['scopes']
    )

    last_history_id = user_data.get('last_history_id', '1')

    gmail_service = build('gmail', 'v1', credentials=credentials)
    
    try:
        history = gmail_service.users().history().list(userId=user_email, startHistoryId=last_history_id).execute()

        for event in history.get('history', []):
            for added_message in event.get('messagesAdded', []):
                try:
                    msg = gmail_service.users().messages().get(userId=user_email, id=added_message['message']['id'], format='full').execute()
                    for head in msg['payload']['headers']:
                        if head['name'] == 'From':
                            sender = head['value']
                        elif head['name'] == 'Date':
                            receive_datetime = head['value']
                    payload = msg.get('payload', '')
                    if payload:
                        if 'parts' in payload:
                            for part in payload['parts']:
                                if part['mimeType'] == 'text/plain':
                                    msg_content = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                        elif 'body' in payload:
                            msg_content = base64.urlsafe_b64decode(payload['body']['data']).decode('utf-8')
                    message_details = {
                        'data' : msg_content,
                        'sender' : sender,
                        'datetime' : receive_datetime
                    }
                    process_and_store_email(message_details, user_email)
                except Exception as e:
                    print(f"Error processing message: {e}")

        user_ref.update({'last_history_id': new_history_id})
        print(f"Successfully processed new emails for {user_email}")
    except Exception as e:
        print(f"Error fetching history: {e}")
        if 'last_history_id' not in user_data:
            user_ref.update({'last_history_id': new_history_id})
            print(f"Initialized last_history_id to {new_history_id}")

def process_and_store_email(message_details, user_email):
    # Extract email content
    content = message_details.get('data', '')  # Use full message body in practice
    '''
    # Call Vertex AI model for entity extraction
    # model = aiplatform.TextGenerationModel.from_pretrained("text-bison@001")
    model = GenerativeModel("gemini-1.5-flash-002")
    prompt = f"""From the below given email content, Please determine if the email is sent by a recruiter or not. 
            If yes please Extract company name, application status(Applied, Reject, Online Assessment, Interview, Offer), and due date in case of Online Assessment from this email: {content}
            
            Provide Output strictily in the following format
            Company_name#"Actual_company_name",
            Status#"Applied/Reject/Online Assessment/Interview/Offer",
            Due_Date#"yyyy-mm-dd" In case of Online Assessment or Interview

            just guve JSON output with the following keys "Relevant", "Company_name", "Status", "Due_Date"

            if the email is not related to applied Job then Relevant field should be False else True
            """
    response = model.generate_content(prompt)
    # response = model.predict(prompt)
    
    # Parse the Vertex AI response (this will depend on how your model is set up)
    extracted_data = parse_vertex_ai_response(response)

    if extracted_data:
        db.collection('processed_emails').add({
            'user_email': user_email,
            'company_name': extracted_data.get('company_name'),
            'status': extracted_data.get('status'),
            'due_date': extracted_data.get('due_date'),
            'original_snippet': content  # Optional: store original snippet for reference
        })
    '''
    publish_message(json.dumps({'email': user_email, 'content': content}))
    
def publish_message(messages):
    psub_client = pubsub_v1.PublisherClient()
    
    topic_path = psub_client.topic_path('midterm-440408', 'unprocessed-emails')

    try:
        message_data = messages.encode("utf-8")
        future = psub_client.publish(topic_path, data=message_data)
        
        print(f"Published message ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred: {e}")

def parse_vertex_ai_response(response):
    # Implement parsing logic based on your model's output format
    # This is a placeholder function; adjust it according to your needs
    print(response)
    return ""
    return {
        'company_name': 'Example Company',
        'status': 'Applied',
        'due_date': '2023-12-31'
    }