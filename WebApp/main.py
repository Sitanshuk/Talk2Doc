import os
import logging
from flask import Flask, render_template, redirect, url_for, request, session
from google_auth_oauthlib.flow import Flow
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
import firebase_admin
from firebase_admin import credentials, firestore

app = Flask(__name__)
app.secret_key = os.urandom(24)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'

cred = credentials.Certificate("secrets/midterm-440408-firebase-adminsdk-2le9q-cf61d3eade.json")
firebase_admin.initialize_app(cred)
db = firestore.client()

# Configuration
CLIENT_SECRETS_FILE = "secrets/client_secret_889977581797-0kh90c8n9n5c9lotd1pjpp0kv46dka56.apps.googleusercontent.com.json"
SCOPES = ['https://www.googleapis.com/auth/userinfo.email',
          'https://www.googleapis.com/auth/gmail.readonly',
          'https://www.googleapis.com/auth/pubsub',
          'openid']

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def save_credentials(credentials):
    user_info = build('oauth2', 'v2', credentials=credentials).userinfo().get().execute()
    user_email = user_info['email']
    user_data = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }

    # Check if the collection exists, if not, create it
    users_collection = db.collection('users')
    if not users_collection.document(user_email).get().exists:
        users_collection.document(user_email).set({})

    # Now update the document
    users_collection.document(user_email).set(user_data, merge=True)

    # Set up Gmail watch
    watch_response = setup_gmail_watch(credentials, user_email)
    if watch_response:
        users_collection.document(user_email).set({'watch_expiration': watch_response['expiration']}, merge=True)

    return user_email
def get_credentials(user_email):
    user_doc = db.collection('users').document(user_email).get()
    if user_doc.exists:
        user_data = user_doc.to_dict()
        credentials = Credentials(
            token=user_data['token'],
            refresh_token=user_data['refresh_token'],
            token_uri=user_data['token_uri'],
            client_id=user_data['client_id'],
            client_secret=user_data['client_secret'],
            scopes=user_data['scopes']
        )
        return refresh_token_if_expired(credentials)
    return None

def refresh_token_if_expired(credentials):
    if credentials and credentials.expired and credentials.refresh_token:
        try:
            credentials.refresh(Request())
            db.collection('users').document(credentials.id_token['email']).update({
                'token': credentials.token,
                'refresh_token': credentials.refresh_token
            })
        except Exception as e:
            logger.error(f"Error refreshing token: {str(e)}")
    return credentials

def setup_gmail_watch(credentials, user_email):
    gmail_service = build('gmail', 'v1', credentials=credentials)
    request = {
        'labelIds': ['INBOX'],
        'topicName': 'projects/midterm-440408/topics/gmail-notification'
    }
    try:
        response = gmail_service.users().watch(userId=user_email, body=request).execute()
        expiration = response.get('expiration')
        if expiration:
            db.collection('users').document(user_email).update({
                'gmail_watch_expiration': expiration
            })
        return response
    except HttpError as error:
        logger.error(f"Error setting up Gmail watch: {str(error)}")
        return None

@app.route('/')
def index():
    user_email = session.get('user_email')
    if user_email:
        credentials = get_credentials(user_email)
        if credentials:
            return render_template('index.html', authorized=True, user_email=user_email)
    return render_template('index.html', authorized=False)

@app.route('/login')
def login():
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRETS_FILE, scopes=SCOPES)
    flow.redirect_uri = url_for('oauth2callback', _external=True)
    authorization_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true', prompt='consent')
    session['state'] = state
    return redirect(authorization_url)

@app.route('/chat')
def chat():
    return render_template('chat.html')

@app.route('/settings')
def settings():
    user_email = session.get('user_email')
    gmail_authorized = False
    notion_authorized = False
    if user_email:
        credentials = get_credentials(user_email)
        if credentials:
            gmail_authorized = True
    notion_authorized = session.get('notion_authorized', False)
    return render_template('settings.html', gmail_authorized=gmail_authorized, notion_authorized=notion_authorized)

@app.route('/oauth2callback')
def oauth2callback():
    try:
        state = session['state']
        flow = Flow.from_client_secrets_file(CLIENT_SECRETS_FILE, scopes=SCOPES, state=state)
        flow.redirect_uri = url_for('oauth2callback', _external=True)
        authorization_response = request.url
        flow.fetch_token(authorization_response=authorization_response)
        credentials = flow.credentials
        user_info = build('oauth2', 'v2', credentials=credentials).userinfo().get().execute()
        session['user_email'] = user_info['email']
        save_credentials(credentials)
        return redirect(url_for('settings'))
    except Exception as e:
        logger.error(f"Error in OAuth callback: {str(e)}")
        return "Error during authorization. Please try again.", 400

@app.route('/authorize_notion')
def authorize_notion():
    # Implement Notion authorization here
    session['notion_authorized'] = True
    return redirect(url_for('settings'))

@app.route('/revoke_gmail')
def revoke_gmail():
    user_email = session.pop('user_email', None)
    if user_email:
        db.collection('users').document(user_email).delete()
    return redirect(url_for('settings'))

@app.route('/revoke_notion')
def revoke_notion():
    session['notion_authorized'] = False
    return redirect(url_for('settings'))

@app.route('/renew_watch')
def renew_watch():
    user_email = session.get('user_email')
    if not user_email:
        return redirect(url_for('login'))
    credentials = get_credentials(user_email)
    if not credentials:
        return redirect(url_for('login'))
    watch_response = setup_gmail_watch(credentials, user_email)
    if watch_response:
        return redirect(url_for('settings'))
    else:
        return "Failed to renew watch", 400

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

if __name__ == '__main__':

    # app.run(port=5000, debug=True) #For Local
    app.run(host='0.0.0.0', port=8080, debug=True)