from __future__ import annotations
import os
import fitz
import json
import hashlib
import requests
import functions_framework
from random import randrange

from flask import jsonify, request
from google.cloud import aiplatform_v1beta1
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from google.cloud.aiplatform_v1beta1.services.index_service import IndexServiceClient
from google.cloud.aiplatform_v1beta1.types import IndexDatapoint, UpsertDatapointsRequest

from firebase_admin import firestore

def store_page_details(db, page_details):
    page_doc = db.collection('page-details').document(page_details['id']).set({
            'page_id': page_details['id'],
            'last_edited_time': page_details['last_edited_time']
        })
    return "Success"

def get_batched_page_details(db, batch):
    processed_batch = []
    page_ids = [page['id'] for page in batch]

    # docs = db.collection('page-details').where(firestore.FieldPath.document_id(), 'in', page_ids).get()
    docs = db.collection('page-details').where('page_id', 'in', page_ids).get()
    fs_page_details = {}
    for doc in docs:
        if doc.exists:
            fs_page_details[doc.to_dict()['page_id']] = doc.to_dict()['last_edited_time']
    
    for page in batch:
        try:
            curr_pid = page['id']
            curr_let = page['last_edited_time']
            if (curr_pid in fs_page_details and curr_let > fs_page_details[curr_pid]) or curr_pid not in fs_page_details:
                print(f"\n^^^^^^^^^^^^^^^^^ Found matching page or new page - {page} ^^^^^^^^^^^^^^^^^")
                print(f"\n^^^^^^^^^^^^^^^^^ From Firestore - {fs_page_details} ^^^^^^^^^^^^^^^^^")
                processed_batch.append(page)
        except Exception as e:
            print(f"################# This is the Error page - {page}################")

    return processed_batch

class ReadNotionDB:
    def __init__(
        self,
        email = None,
        notion_token = None,
        page_id = None,
        database_id = None
    ):
        self.url = "https://api.notion.com/v1"
        self.email = email
        if notion_token:
            self.NOTION_API_KEY = notion_token
        else:    
            self.NOTION_API_KEY = os.getenv('NOTION_SECRET')
        
        self.headers = {
            "Authorization": f"Bearer {self.NOTION_API_KEY}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28"
        }
        self.last_read_timestamp = None
        if page_id:
            self.PAGE_ID = page_id
        else:
            self.PAGE_ID = os.getenv('NOTION_DATABASE')

        if database_id:
            self.DATABASE_ID = database_id
        # else:
        #     self.DATABASE_ID = self.get_inline_database_id()

    def fetch_sub_pages(self):
        """
        Fetch all sub-pages of a given page ID.
        """
        sub_pages = []
        url = f"{self.url}/blocks/{self.PAGE_ID}/children"
        
        response = requests.get(url, headers=self.headers)
        print(f"Responses: {response}, sub-pages")
        if response.status_code == 200:
            results = response.json().get('results', [])
            for block in results:
                # print(block['type'])
                if block['type'] == 'child_page':  # Ensure it's a sub-page
                    sub_pages.append({
                        "id": block['id'],
                        "title": block['child_page']['title'],
                        "last_edited_time": block['last_edited_time']
                    })
        else:
            print(f"########## Error - {self.email}, {self.NOTION_API_KEY}, {self.PAGE_ID} #####################")
            raise Exception(f"Failed to fetch sub-pages: {response.status_code}, {response.text}")

        return sub_pages
    
    def read_page_content(self, pages):
        """
        Read the content of the given list of page IDs.
        """
        pages_content = {}
        for page in pages:
            url = f"{self.url}/blocks/{page['id']}/children"
            
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                pages_content[page['id']] = response.json().get('results', [])
            else:
                raise Exception(f"Failed to fetch content for page ID {page['id']}: {response.status_code}, {response.text}")

        return pages_content
    
    def get_uploaded_files(self, page_id):
        """
        Fetch uploaded files (like PDFs) from a Notion page.

        :param page_id: The ID of the Notion page
        :return: A list of file URLs and their names
        """
        uploaded_files = []
        url = f"{self.url}/blocks/{page_id}/children"
        
        response = requests.get(url, headers=self.headers)
        if response.status_code == 200:
            all_blocks = response.json().get('results', [])
            
            for block in all_blocks:
                # Check if the block is a file block
                if block['type'] == 'file':
                    print(json.dumps(block))
                    file_info = block['file']
                    uploaded_files.append({
                        "name": file_info.get('name', 'Unnamed File'),
                        "url": file_info['file']['url']
                    })
                # If PDFs are stored inside child pages or databases, recursive handling might be required
                
        else:
            raise Exception(f"Failed to fetch content for page ID {page_id}: {response.status_code}, {response.text}")

        return uploaded_files

    def read_page_content_with_timestamp(self, pages):
        """
        Read content of pages, only fetching blocks created after the last function call.
        Updates the last read timestamp to the created_time of the most recent block read.
        Filters out empty blocks (blocks without text or meaningful content).
        """
        pages_content = []
        latest_timestamp = None  # To track the most recent created_time of the blocks
        
        for page in pages:
            url = f"{self.url}/blocks/{page['id']}/children"
            
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                all_blocks = response.json().get('results', [])
                
                # Filter blocks by creation time if last read time is available
                if self.last_read_timestamp:
                    filtered_blocks = [
                        block for block in all_blocks
                        if block['created_time'] > self.last_read_timestamp
                    ]
                else:
                    filtered_blocks = all_blocks

                # Filter out empty blocks
                non_empty_blocks = [
                    block for block in filtered_blocks
                    if ('rich_text' in block.get("paragraph", {}) and block.get("paragraph",{}).get("rich_text",[])) or block['type'] == "file"
                ]
                # Add non-empty blocks to result
                for block in non_empty_blocks:
                    # print(block)
                    try:
                        if block['type'] == "file":
                            content_type = "file"
                            content = block['file']['file']['url']
                        else:
                            content_type = "text"
                            text_parts = block['paragraph']['rich_text']
                            content = "".join([text_part['plain_text']for text_part in text_parts])
                    
                        pages_content.append({
                            'page_title': page['title'],
                            'content': content,
                            'email': self.email,
                            'created_time': block['created_time'],
                            'content_type':content_type
                        })
                    except Exception as e:
                        print(json.dumps({
                            "Error": e,
                            "Block": block
                        }))

                # Update latest timestamp based on the blocks read
                for block in non_empty_blocks:
                    block_created_time = block['created_time']
                    if not latest_timestamp or block_created_time > latest_timestamp:
                        latest_timestamp = block_created_time
            else:
                print("In error")
                raise Exception(f"Failed to fetch content for page ID {page['id']}: {response.status_code}, {response.text}")
        
        # Update the last read timestamp to the most recent block's created_time
        self.last_read_timestamp = latest_timestamp if latest_timestamp else self.last_read_timestamp

        return pages_content
    
    def read_entire_sub_page(self, pages):
        """
        Read content of pages, only fetching blocks created after the last function call.
        Updates the last read timestamp to the created_time of the most recent block read.
        Filters out empty blocks (blocks without text or meaningful content).
        """
        pages_content = []
        latest_timestamp = None  # To track the most recent created_time of the blocks
        print(f"Pages: {pages}")
        for page in pages:
            page_dict_to_embed = {
                "page_id": page['id'],
                "page_title": page['title'],
                "content": "",
                "files": [],
                "last_updated": ""
            }

            url = f"{self.url}/blocks/{page['id']}/children"
            
            response = requests.get(url, headers=self.headers)
            print(f"Response: ---------------- {response}")
            if response.status_code == 200:
                all_blocks = response.json().get('results', [])

                # Filter out empty blocks
                non_empty_blocks = [
                    block for block in all_blocks
                    if ('rich_text' in block.get("paragraph", {}) and block.get("paragraph",{}).get("rich_text",[])) or block['type'] == "file"
                ]
                # Add non-empty blocks to result
                for block in non_empty_blocks:
                    # print(block)
                    try:
                        if block['type'] == "file":
                            # Add file to list of files for this page
                            page_dict_to_embed['files'].append(block['file']['file']['url'])
                        else:
                            # Concat the string to the whole content
                            text_parts = block['paragraph']['rich_text']
                            content = "".join([text_part['plain_text']for text_part in text_parts])
                            page_dict_to_embed['content'] += " " + content

                        # Add the latest update time to the dict
                        block_created_time = block['created_time']
                        if page_dict_to_embed['last_updated'] < block_created_time:
                            page_dict_to_embed['last_updated'] = block_created_time

                    except Exception as e:
                        print(json.dumps({
                            "Error": e,
                            "Block": block
                        }))

                pages_content.append(page_dict_to_embed)
            else:
                print("In error")
                raise Exception(f"Failed to fetch content for page ID {page['id']}: {response.status_code}, {response.text}")

        return pages_content

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

def embed_text(input_texts, task = "RETRIEVAL_DOCUMENT") -> list[list[float]]:
    """
    Generate embeddings for a list of texts using Vertex AI Model Garden's pre-trained model.

    Args:
        input_texts (list[str]): A list of texts to be embedded.
        dimensionality (int): Dimensionality of the output embeddings. Default is 256.
        task (str): Task type for embedding. Default is "RETRIEVAL_DOCUMENT".

    Returns:
        list[list[float]]: List of embedding vectors for each input text.
    """
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    inputs = [TextEmbeddingInput(text=text, task_type=task) for text in input_texts]
    embeddings = model.get_embeddings(inputs)
    #print(embeddings)
    return [embedding.values for embedding in embeddings]

def upload_embeddings(json_data, embeddings, project_id, region, index_id):
    """
    Upload embeddings to Google Cloud Matching Engine.

    Args:
        json_data (list): Original data with metadata (user_email, page_title, content, created_time, etc.).
        embeddings (list): List of generated embeddings.
        project_id (str): GCP project ID.
        region (str): GCP region.
        index_id (str): Matching Engine Index ID.

    Returns:
        str: Status of the operation.
    """

    #client = aiplatform_v1beta1.IndexEndpointServiceClient()
    #response = client.list_index_endpoints(parent=f"projects/{project_id}/locations/{region}")
    #print("HELLLOOOOOO" + response)

    client = IndexServiceClient(client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"})
    index_name = f"projects/{project_id}/locations/{region}/indexes/{index_id}"

    datapoints = []
    for i, item in enumerate(json_data):
        namespace = item["user_email"]
        content_hash = hashlib.md5(item["content"].encode()).hexdigest()
        datapoint_id = f"{item['user_email']}-{content_hash}"

        restrictions = [
            IndexDatapoint.Restriction(namespace="page_title", allow_list=[item["page_title"]]),
            IndexDatapoint.Restriction(namespace="content", allow_list=[item["content"]]),
            IndexDatapoint.Restriction(namespace="created_time", allow_list=[item["last_updated"]]),
            IndexDatapoint.Restriction(namespace="user_email", allow_list=[item["user_email"]])
        ]

        datapoint = IndexDatapoint(
            datapoint_id=datapoint_id,
            feature_vector=embeddings[i],
            restricts=restrictions
        )

        datapoints.append(datapoint)
    
    print("\n\n######################################### Starting to upsert #########################################")
    request = UpsertDatapointsRequest(index=index_name, datapoints=datapoints)
    client.upsert_datapoints(request=request)
    """request = UpsertDatapointsRequest(
        index=index_name,
        datapoints=[datapoint]
    )"""
    print("\n\n######################################### Finished to upsert #########################################")
    return "Embeddings successfully uploaded to Matching Engine."

def filter_updated_pages(pages):
    final_processed_pages = []
    db = firestore.Client()
    batched_pages = [pages[i:i + 10] for i in range(0, len(pages), 10)]
    print(f"Batched pages: {batched_pages}")
    for batch in batched_pages:
        processed_batch = get_batched_page_details(db,batch)
        final_processed_pages.extend(processed_batch)

    for page in final_processed_pages:
        store_page_details(db,page)

    # Final processed pages has the pages that im supposed to query 
    print(final_processed_pages)
    return final_processed_pages

def get_notion_updates(creds_json):
    # get all user emails from firestore

    email = creds_json['user_email']
    notion_token = creds_json['notion_token']
    page_id = creds_json['page_id']

    notion_user = ReadNotionDB(email,notion_token=notion_token, page_id=page_id)
    pages = notion_user.fetch_sub_pages()

    # Filter pages based on last_edited_time
    filtered_pages = filter_updated_pages(pages)

    updated_content = notion_user.read_entire_sub_page(filtered_pages)
    print(f"Updated Contents: {updated_content}")
    key = "user_email"
    value = email
    for page in updated_content:
        page[key] = value
    # new_file_blocks = [block for block in new_blocks if block['content_type'] == 'file']
    # new_text_blocks = [block for block in new_blocks if block['content_type'] == 'text']
    return updated_content

def process_file_blocks(files):
    file_content = ""
    try:
        for pdf in files:
            resp = requests.get(pdf)
            tfile_name = f'/tmp/notion-file{randrange(1,1000)}.pdf'
            
            if resp.status_code != 200:
                return

            with open(tfile_name, 'wb') as file:
                file.write(resp.content)
            
            doc = fitz.open(tfile_name)

            file_content = "".join([pg.get_text() for pg in doc])
            print(f"$$$$$$$$$$$$$ File Content - {file_content} \n $$$$$$$$$$$$$$$$ PDF link - {pdf}")
            doc.close()
    except Exception as err:
        print("\n=============== ERROR FROM FILE - ",err)
        return ""

    return file_content

def process_and_store_embeddings(request):
    """
    Cloud Function entry point. Processes JSON input, generates embeddings,
    and uploads them to Matching Engine.
    """
    request_json = request.get_json(silent=True)
    if not request_json:
        return jsonify({"error": "Invalid input. JSON data is required."}), 400

    project_id = "midterm-440408"
    region = "us-central1"
    index_id = "1201225249438302208"

    if not (project_id and index_id):
        return jsonify({"error": "Missing required headers: Project-ID or Index-ID"}), 400

    # Process each user batch
    for user_creds in request_json['user_batch']:
        updated_content = get_notion_updates(user_creds)
        if updated_content == []:
            continue
        print(f"################### Filtered Page Content: {json.dumps(updated_content)}")

        all_chunks_metadata = []
        all_embeddings = []

        # Process each page
        for page in updated_content:
            page_id = page["page_id"]
            email = page["user_email"]

            # Combine content with file content
            page["content"] += " " + process_file_blocks(page.get("files", []))

            # Create overlapping chunks
            chunk_size = 1000
            overlap = 100
            chunks = create_overlapping_character_chunks(page["content"], chunk_size, overlap)

            # Generate embeddings and prepare metadata
            for idx, chunk in enumerate(chunks, start=1):
                datapoint_id = f"{email}-{page_id}-{idx}"  # Generate unique datapoint_id
                embedding = embed_text([chunk])[0]  # Generate embedding for the chunk

                all_chunks_metadata.append({
                    "user_email": email,
                    "page_id": page_id,
                    "content": chunk,
                    "page_title": page["page_title"],
                    "last_updated": page["last_updated"],
                    "datapoint_id": datapoint_id
                })
                all_embeddings.append(embedding)

        # Upload embeddings to Matching Engine
        status = upload_embeddings_v2(all_chunks_metadata, all_embeddings, project_id, region, index_id)
        print(f"\n\n######################################### {status} #########################################")

    return jsonify({"message": "Embeddings successfully processed and uploaded."}), 200

def create_overlapping_character_chunks(input_text, chunk_size, overlap):
    """
    Create overlapping chunks from a single input string based on character counts.

    Args:
        input_text (str): The input string to be chunked.
        chunk_size (int): Maximum number of characters in each chunk.
        overlap (int): Number of overlapping characters between consecutive chunks.

    Returns:
        list: List of overlapping text chunks.
    """
    chunks = []
    start = 0
    while start < len(input_text):
        end = min(start + chunk_size, len(input_text))
        chunk = input_text[start:end]
        chunks.append(chunk)
        start += chunk_size - overlap  # Move start forward with overlap
    return chunks


def upload_embeddings_v2(json_data, embeddings, project_id, region, index_id):
    """
    Upload embeddings to Google Cloud Matching Engine.

    Args:
        json_data (list): Original data with metadata (user_email, page_title, content, last_updated, etc.).
        embeddings (list): List of generated embeddings.
        project_id (str): GCP project ID.
        region (str): GCP region.
        index_id (str): Matching Engine Index ID.

    Returns:
        str: Status of the operation.
    """
    client = IndexServiceClient(client_options={"api_endpoint": "us-central1-aiplatform.googleapis.com"})
    index_name = f"projects/{project_id}/locations/{region}/indexes/{index_id}"

    datapoints = []
    print(json_data)
    for i, item in enumerate(json_data):
        datapoint_id = item["datapoint_id"]

        restrictions = [
            IndexDatapoint.Restriction(namespace="page_title", allow_list=[item["page_title"]]),
            IndexDatapoint.Restriction(namespace="content", allow_list=[item["content"]]),
            IndexDatapoint.Restriction(namespace="last_updated", allow_list=[item["last_updated"]]),
            IndexDatapoint.Restriction(namespace="user_email", allow_list=[item["user_email"]])
        ]

        datapoint = IndexDatapoint(
            datapoint_id=datapoint_id,
            feature_vector=embeddings[i],
            restricts=restrictions
        )
        print(f"Datapoints: {datapoint}")
        datapoints.append(datapoint)

    print("\n\n######################################### Starting to upsert #########################################")
    request = UpsertDatapointsRequest(index=index_name, datapoints=datapoints)
    client.upsert_datapoints(request=request)
    print("\n\n######################################### Finished to upsert #########################################")
    return "Embeddings successfully uploaded to Matching Engine."
