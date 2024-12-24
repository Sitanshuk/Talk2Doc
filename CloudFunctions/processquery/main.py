from __future__ import annotations
import vertexai
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel, TextGenerationModel, ChatModel
from vertexai.generative_models import GenerativeModel
from google.cloud import aiplatform
from google.cloud.aiplatform_v1beta1.types import IndexDatapoint, UpsertDatapointsRequest
from google.cloud.aiplatform_v1beta1.services.index_service import IndexServiceClient
from google.cloud.aiplatform.matching_engine.matching_engine_index_endpoint import Namespace
import hashlib
import json
from flask import jsonify, request

model = GenerativeModel("gemini-1.5-flash-002")

def embed_text(input_text, task = "QUESTION_ANSWERING") -> list[list[float]]:
    """
    Generate embeddings for a user query using Vertex AI Model Garden's pre-trained model.

    Args:
        input_texts (list[str]): A list of texts to be embedded.
        dimensionality (int): Dimensionality of the output embeddings. Default is 256.
        task (str): Task type for embedding. Default is "QUESTION_ANSWERING".

    Returns:
        list[list[float]]: List of embedding vectors for each input text.
    """
    model = TextEmbeddingModel.from_pretrained("text-embedding-005")
    #inputs = [TextEmbeddingInput(text=text, task_type=task) for text in input_texts]
    #embeddings = model.get_embeddings(inputs)

    inputs = [TextEmbeddingInput(text=text, task_type=task) for text in input_text]
    embeddings = model.get_embeddings(inputs)

    return [embedding.values for embedding in embeddings]


def query_user_embeddings(user_email, query_embedding, project_id, region, endpoint_id, index_id, top_k = 5):
    """
    Query embeddings for a specific user and perform similarity search.

    Args:
        user_email (str): The email of the user whose embeddings to query.
        query_text (str): The text to query for similarity search.
        project_id (str): GCP project ID.
        region (str): GCP region.
        index_id (str): Matching Engine Index ID.
        top_k (int): Number of top results to retrieve.

    Returns:
        list[dict]: Top-K similar embeddings with metadata.
    """
    aiplatform.init(project=project_id, location=region)
    vertexai.init(project=project_id, location=region)
    index_endpoint = aiplatform.MatchingEngineIndexEndpoint(index_endpoint_name=f"projects/{project_id}/locations/{region}/indexEndpoints/{endpoint_id}")

    #Filter out by emails
    filter_list = [
    Namespace(
        "user_email",  # The metadata field to filter on
        [user_email],  # The allowed values for this namespace
        []  # Deny list can be empty if not used
        )
    ]
    try:
        response = index_endpoint.find_neighbors(
            deployed_index_id=index_id,
            queries=[query_embedding],
            num_neighbors=top_k,
            filter=filter_list
        )

        #print(response)

        results = [
            {
                "datapoint_id": neighbor.id,
                "distance": neighbor.distance
            }
            for neighbor in response[0]
        ]
    except:
        print("Oops! Unfortunately we don't have any relevant data that we could pull from your notes!\nTry updating your notes!")
        return "Oops! Unfortunately we don't have any relevant data that we could pull from your notes!\nTry updating your notes!"

    neighbor_ids = [result["datapoint_id"] for result in results]
    
    contents = index_endpoint.read_index_datapoints(deployed_index_id=index_id, ids=neighbor_ids)

    cleaned_response = []
    for datapoint in contents:
        for entry in datapoint.restricts:
            if entry.namespace == "content":
                cleaned_datapoint = {
                    "datapoint_id": datapoint.datapoint_id,  # Include ID
                    "contents": entry.allow_list[0]         # Include metadata
                }
                cleaned_response.append(cleaned_datapoint)
    print(cleaned_response[0])
    return cleaned_response[0]['contents']

def get_llm_output(text, query):
    """
    Generate output from Vertex AI's pre-trained PaLM model.

    Args:
        text (str): The input text to feed into the LLM.

    Returns:
        str: The LLM's generated output.
    """
    try:
        prompt = """ You are a helpful generative model that will use the text and try to answer the query. Do not halluicnate, stay relevant to the query. \n Here is the query to be processed:- \n
        """
        text_prompt = """\nThis is the relevant text from which you need to answer the query:- \n"""

        a = model.generate_content(prompt + query[0] + text_prompt + text)
        return a.text
    except Exception as e:
        print(f"Error generating output from Vertex AI Chat-Bison: {e}")
        return "There was an error generating a response."

def process_and_query_embeddings(request):
    """
    Cloud Function entry point. Processes JSON input, generates embeddings,
    and uploads them to Matching Engine.

    Args:
        request: Flask request object containing JSON payload.

    Returns:
        Flask response object with status message.
    """
    request_json = request.get_json(silent=True)
    if not request_json:
        return jsonify({"error": "Invalid input. JSON data is required."}), 400

    project_id = "midterm-440408"
    region = "us-central1"
    endpoint_id = "5622211971144220672"
    index_id = "deploy_stream_768_1733596750973"

    print("Yooo!")

    if not (project_id and endpoint_id):
        return jsonify({"error": "Missing required headers: Project-ID or Index-ID"}), 400

    # Extract content for embedding
    query = [request_json['content']]
    user_email = request_json['user_email']
    print(query)

    # Generate embeddings
    embeddings = embed_text(query)
    embeddings = embeddings[0]

    # Get similar embeddings
    output_content = query_user_embeddings(user_email, embeddings, project_id, region, endpoint_id, index_id, 5)
    if output_content == "Oops! Unfortunately we don't have any relevant data that we could pull from your notes!\nTry updating your notes!":
        return jsonify({"response": output_content}), 200

    final_message = get_llm_output(output_content, query)

    return jsonify({"response": final_message}), 200