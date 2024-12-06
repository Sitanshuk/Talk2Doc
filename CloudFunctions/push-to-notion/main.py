import os
import json
import logging
import requests
import firebase_admin
import functions_framework
from flask import Flask, jsonify
# from dotenv import load_dotenv
from datetime import datetime
from firebase_admin import credentials, firestore
from google.cloud import pubsub_v1

db = firestore.Client()

def get_notion_credentials(user_email):
    user_doc = db.collection('users').document(user_email).get()
    if user_doc.exists:
        user_data = user_doc.to_dict()
        return user_data['notion_token'], user_data['notion_page'], None
    return None, None, None
  
class NotionCrud:
    def __init__(
        self,
        notion_token = None,
        page_id = None,
        database_id = None
    ):
        self.url = "https://api.notion.com/v1"

        if notion_token:
            self.NOTION_API_KEY = notion_token
        # else:    
        #     self.NOTION_API_KEY = os.getenv('NOTION_SECRET')
        
        self.headers = {
            "Authorization": f"Bearer {self.NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }

        if page_id:
            self.PAGE_ID = page_id
        # else:
        #     self.PAGE_ID = os.getenv('NOTION_DATABASE')

        if database_id:
            self.DATABASE_ID = database_id
        else:
            self.DATABASE_ID = self.get_inline_database_id()
        
    def get_inline_database_id(self):
        """
        Fetches the database ID of an inline database on a Notion page.

        Parameters:
        - page_id (str): The ID of the Notion page containing the inline database.
        - notion_token (str): Your Notion API integration token.

        Returns:
        - str: The database ID of the inline database, or None if not found.
        """
        url = f"https://api.notion.com/v1/blocks/{self.PAGE_ID}/children"

        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
            # Search for a block of type 'child_database'
            for block in data.get("results", []):
                if block["type"] == "child_database":
                    return block["id"]

            print("No inline database found on the page. Creating new database.")
            database_json = self.create_database("Talk2Doc Job Applications", True)
            return database_json['id']

        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def create_database(
        self,
        PAGE_TITLE,
        IS_INLINE
    ):
        url = f"{self.url}/databases"
        
        payload = {
            "parent": {"type": "page_id", "page_id": self.PAGE_ID},
            "title": [{"type": "text", "text": {"content": PAGE_TITLE}}],
            "is_inline": IS_INLINE,
            "properties": {
                "Company": {"title": {}},
                "Position": {"rich_text": {}},
                "Data of Application": {"date": {}},
                "Status": {
                    "select": {
                        "options": [
                            {"name": "Applied", "color": "blue"},
                            {"name": "OA", "color": "yellow"}, 
                            {"name": "Rejected", "color": "red"},
                            {"name": "Interview", "color": "green"},
                            {"name": "Offer", "color": "purple"}
                        ]
                    }
                },
                "Deadline": {"date": {}},
                "Notes": {"rich_text": {}}
            }
        }

        response = requests.post(url, headers=self.headers, json=payload)

        if response.status_code == 200:
            print("Database created successfully!")
            # print(json.dumps(response.json(), indent=2))
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None

    def create_database_entry(
        self,
        title=None,
        position=None,
        notes=None,
        status=None,
        deadline=None,
        data_of_application=None
    ):
        """
        Dynamically creates an entry in a Notion database.

        Parameters:
        - database_id (str): The ID of the Notion database.
        - notion_token (str): Your Notion API integration token.
        - title (str): The value for the 'Company' (title) property.
        - position (str): The value for the 'Position' (rich_text) property.
        - notes (str): The value for the 'Notes' (rich_text) property.
        - status (str): The value for the 'Status' (select) property.
        - deadline (str): The value for the 'Deadline' (date) property in ISO format (YYYY-MM-DD).
        - data_of_application (str): The value for the 'Data of Application' (date) property in ISO format (YYYY-MM-DD).

        Returns:
        - dict: The created page response from Notion.
        """
        url = f"{self.url}/pages"

        # Build the properties dynamically
        properties = self.get_properties_dict(title, position, notes, status, deadline, data_of_application)

        # Build the payload
        payload = {
            "parent": {"database_id": self.DATABASE_ID},
            "properties": properties
        }

        try:
            print("SENDING REQUEST TO:", url, "with payload:", payload)
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            print("Page created successfully!")
            # return response.json()
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return None

    def get_payload(
        self,
        title,
        position,
        status,
        deadline,
        data_of_application,
        notes
    ):
        filters = []
        if title:
            filters.append({
                "property": "Company",
                "title": {
                    "equals": title
                }
            })
        if position:
            filters.append({
                "property": "Position",
                "rich_text": {
                    "contains": position
                }
            })
        if status:
            filters.append({
                "property": "Status",
                "select": {
                    "equals": status
                }
            })
        if deadline:
            filters.append({
                "property": "Deadline",
                "date": {
                    "equals": deadline
                }
            })
        if data_of_application:
            filters.append({
                "property": "Data of Application",
                "date": {
                    "equals": data_of_application
                }
            })
        if notes:
            filters.append({
                "property": "Notes",
                "rich_text": {
                    "contains": notes
                }
            })

        # If no filters are provided, query all pages
        payload = {"filter": {"and": filters}} if filters else {}
        return payload

    def retrieve_page_dynamic(
        self,
        title=None,
        position=None,
        status=None,
        deadline=None,
        data_of_application=None,
        notes=None
    ):
        """
        Dynamically retrieves a page from a Notion database based on provided properties.

        Parameters:
        - database_id (str): The ID of the Notion database.
        - notion_token (str): Your Notion API integration token.
        - title (str): Filter by 'Company' (title) property.
        - position (str): Filter by 'Position' (rich_text) property.
        - status (str): Filter by 'Status' (select) property.
        - deadline (str): Filter by 'Deadline' (date) property in ISO format (YYYY-MM-DD).
        - data_of_application (str): Filter by 'Data of Application' (date) property in ISO format (YYYY-MM-DD).
        - notes (str): Filter by 'Notes' (rich_text) property.

        Returns:
        - list: The results of the query matching the filters.
        """
        url = f"{self.url}/databases/{self.DATABASE_ID}/query"

        # Build the filter dynamically
        payload = self.get_payload(title, position, status, deadline, data_of_application, notes)

        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json().get("results", [])
        except requests.exceptions.RequestException as e:
            print(f"An error occurred: {e}")
            return []

    def get_properties_dict(
        self,
        title,
        position,
        notes,
        status,
        deadline,
        data_of_application
    ):
        properties = {}
        if title:
            properties["Company"] = {
                "title": [
                    {
                        "type": "text",
                        "text": {"content": title},
                    }
                ]
            }
        if position:
            properties["Position"] = {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": position},
                    }
                ]
            }
        if notes:
            properties["Notes"] = {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": notes},
                    }
                ]
            }
        if status:
            properties["Status"] = {
                "select": {"name": status}
            }
        if deadline:
            properties["Deadline"] = {
                "date": {"start": deadline, "end": None}
            }
        if data_of_application:
            properties["Data of Application"] = {
                "date": {"start": data_of_application, "end": None}
            }
        return properties

    def get_page_id_to_update(
        self,
        title,
        position,
        status,
        deadline,
        data_of_application,
        notes
    ):
        pages = self.retrieve_page_dynamic(title=title, position=position, status=None, deadline=None, data_of_application=None, notes=None)
        if not pages:
            return None
        print(pages)
        return pages[0]['id']

    def check_if_existing_application(
        self,
        title,
        position,
        status
    ):
        status_order_dict = {
            "Applied": 0,
            "OA": 1,
            "Interview": 2,
            "Offer": 3,
            "Rejected": 4,
        }
        
        pages = self.retrieve_page_dynamic(title=title, position=position)
        if pages:
            # if title and position already exist then check the following: 
            # if new status from the email is less than existing status, then it is a new application
            # if new status from the email is greater than existing status, then it is an update
            existing_status = pages[0]['properties']['Status']['select']['name']
            if status_order_dict[status] > status_order_dict[existing_status]:
                return pages[0]['id']
            else:
                return None
        else:
            return None

    def update_notion_page(
        self,
        page_id,
        title=None,
        position=None,
        notes=None,
        status=None,
        deadline=None,
        data_of_application=None
    ):
        """
        Dynamically updates properties of a Notion page based on the provided arguments.

        Parameters:
        - page_id (str): The ID of the Notion page to update.
        - notion_token (str): Your Notion API integration token.
        - title (str): The value to update the 'Company' (title) property.
        - position (str): The value to update the 'Position' (rich_text) property.
        - notes (str): The value to update the 'Notes' property.
        - status (str): The value to update the 'Status' (select) property.
        - deadline (str): The value to update the 'Deadline' (date) property in ISO format (YYYY-MM-DD).
        - data_of_application (str): The value to update the 'Data of Application' (date) property in ISO format (YYYY-MM-DD).

        Returns:
        - dict: The updated page response from Notion.
        """

        url = f"{self.url}/pages/{page_id}"

        # Build the properties dynamically
        properties = self.get_properties_dict(title, position, notes, status, deadline, data_of_application)

        # Send the PATCH request if there are updates to make
        if properties:
            payload = {"properties": properties}
            try:
                response = requests.patch(url, headers=self.headers, json=payload)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"An error occurred: {e}")
                return None
        else:
            print("No properties to update.")
            return None

def process_message(message_data):
    email = message_data.get('email', None)
    title = message_data.get('title', None)
    position = message_data.get('position', None)
    status = message_data.get('status', None)
    notes = message_data.get('notes', None)
    deadline = message_data.get('deadline', None)
    data_of_application = message_data.get('data_of_application', None)

    if email is None:
        print("No email found in message data")
        return -1
    if title is None:
        print("No title found in message data")
        return -1
    if position is None:
        print("No position found in message data")
        return -1

    # if notes is None:
    #     print("No notes found in message data")
    #     return
    
    notion_token, notion_page, notion_database = get_notion_credentials(email)

    try:
        notion_user = NotionCrud(notion_token=notion_token, page_id=notion_page, database_id=notion_database)
        
        # Check if its a new application or an update
        page_id = notion_user.check_if_existing_application(title, position, status)
        if page_id:
            print("Updating existing application")
            notion_user.update_notion_page(page_id=page_id, status=status, deadline=deadline, data_of_application=data_of_application, notes=notes)
        else:
            print("Creating new application")
            if data_of_application is None:
                print("No data of application found in message data")
                data_of_application = datetime.now().strftime("%Y-%m-%d")
            notion_user.create_database_entry(title=title, position=position, status=status, deadline=deadline, data_of_application=data_of_application, notes=notes)
        return 1
    except Exception as e:
        print(f"An error occurred: {e}")
        return -1

def pull_pubsub_messages(request):
    """
    HTTP-triggered Cloud Function to pull and process messages from Pub/Sub.
    """
    # Replace with your GCP project ID and subscription name
    project_id = "midterm-440408"
    subscription_id = "processed-emails-sub"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    try:
        # Pull messages
        response = subscriber.pull(
            request={"subscription": subscription_path, "max_messages": 10}
        )

        successfully_processed_messages = []
        error_messages = []
        for received_message in response.received_messages:
            message_data = received_message.message.data.decode("utf-8")
            message_data = json.loads(message_data)
            if process_message(message_data) == -1:
                error_messages.append(received_message)
            else:
                successfully_processed_messages.append(received_message)
        
        # Acknowledge the messages that were successfully processed
        for message in successfully_processed_messages:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": [received_message.ack_id],
                }
            )
        status = "success" if len(error_messages) == 0 else "partial"
        success_messages = [json.loads(received_message.message.data.decode("utf-8")) for received_message in successfully_processed_messages]
        error_messages = [json.loads(received_message.message.data.decode("utf-8")) for received_message in error_messages]
        return jsonify({"status": status, "success_messages": success_messages, "error_messages": error_messages}), 200

    except Exception as e:
        print(f"Error pulling messages: {e}")
        return jsonify({"status": "error", "error": str(e)}), 500