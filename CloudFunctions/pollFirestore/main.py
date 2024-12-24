import aiohttp
import asyncio
from google.cloud import firestore
from flask import jsonify

def poll_firestore(event):
    """
    Cloud Function entry point for periodic polling.
    Triggered by Pub/Sub messages.
    """
    # Initialize Firestore client
    db = firestore.Client()

    # Run the main processing logic
    asyncio.run(main(db))

async def main(db, batch_size=10):
    """Fetch user data from Firestore and send to processjson endpoint."""
    # Fetch Firestore data
    docs = db.collection('users').stream()
    data = [
        {
            "user_email": doc.id,  # Assuming email is the document ID
            "notion_token": doc.to_dict().get("notion_token"),
            "page_id": doc.to_dict().get("notion_notes_page")
        }
        for doc in docs
    ]
    print(data)

    if not data:
        print("No data found in Firestore.")
        return

    print(f"Fetched {len(data)} records from Firestore.")

    # Split data into batches
    def batch_data(data, batch_size):
        """Split data into batches of specified size."""
        for i in range(0, len(data), batch_size):
            yield data[i:i + batch_size]

    # Async function to send a single batch
    async def send_batch(batch):
        """Send a batch to the processjson endpoint."""
        url = "https://processjson-889977581797.us-central1.run.app"
        headers = {
            "Authorization": "bearer $(gcloud auth print-identity-token)",
            "Content-Type": "application/json"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json={'user_batch': batch}) as response:
                if response.status == 200:
                    print(f"Batch processed successfully: {await response.text()}")
                else:
                    print(f"Failed to process batch: {response.status} - {await response.text()}")

    # Process all batches concurrently
    tasks = []
    for batch in batch_data(data, batch_size):
        tasks.append(send_batch(batch))

    await asyncio.gather(*tasks)
    print("All batches processed.")
    status = "All batches processed."
    return jsonify({"status": status}), 200
