import base64
import functions_framework
import json
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.cloud import firestore

# Initialize Firestore client
db = firestore.Client()

@functions_framework.cloud_event
def gmail_webhook(cloud_event):
    """
    Process Gmail push notifications and store new emails in Firestore.

    This function is triggered by Gmail push notifications sent to a Pub/Sub topic.
    It retrieves new emails using the Gmail API, saves them to Firestore,
    and updates the last processed history ID for the user.

    Args:
        cloud_event (CloudEvent): The cloud event object containing the Pub/Sub message.

    The function performs the following steps:
    1. Decodes the Pub/Sub message to extract user email and new history ID.
    2. Retrieves user credentials from Firestore.
    3. Fetches new emails using the Gmail API.
    4. Saves raw email data to the 'raw_emails' collection in Firestore.
    5. Updates the user's last processed history ID in Firestore.

    Errors are logged for missing user data, API request failures, and message processing issues.
    """
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

    # Get the last processed history_id or use a default value
    last_history_id = user_data.get('last_history_id', '1')

    # Fetch new emails
    gmail_service = build('gmail', 'v1', credentials=credentials)

    try:
        history = gmail_service.users().history().list(userId=user_email, startHistoryId=last_history_id).execute()
        for event in history.get('history', []):
            for added_message in event.get('messagesAdded', []):
                try:
                    msg = gmail_service.users().messages().get(userId=user_email,
                                                               id=added_message['message']['id']).execute()
                    # Save raw email to Firestore
                    save_raw_email(msg, user_email)
                except Exception as e:
                    print(f"Error processing message: {e}")

        # Update the last processed history_id
        user_ref.update({'last_history_id': new_history_id})
        print(f"Successfully processed new emails for {user_email}")
    except Exception as e:
        print(f"Error fetching history: {e}")
        # If this is the first time and there's no history, set the initial history_id
        if 'last_history_id' not in user_data:
            user_ref.update({'last_history_id': new_history_id})
            print(f"Initialized last_history_id")


def save_raw_email(msg, user_email):
    # Extract relevant information from the email
    subject = next((header['value'] for header in msg['payload']['headers'] if header['name'].lower() == 'subject'),
                   'No Subject')
    sender = next((header['value'] for header in msg['payload']['headers'] if header['name'].lower() == 'from'),
                  'Unknown Sender')

    # Save to 'raw_emails' collection
    db.collection('raw_emails').add({
        'user_email': user_email,
        'message_id': msg['id'],
        'thread_id': msg['threadId'],
        'subject': subject,
        'sender': sender,
        'snippet': msg['snippet'],
        'timestamp': msg['internalDate'],
        'raw_data': msg  # This saves the entire raw message data
    })

    print(f"Saved raw email: Subject: {subject}, From: {sender}")