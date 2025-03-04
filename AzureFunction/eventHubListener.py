import logging
import json
from azure.eventhub import EventHubConsumerClient
from azure.functions import EventHubEvent
from azure.search.documents import SearchClient
from azure.search.documents.indexes.models import SearchIndexClient, SearchIndex

# Azure AI Search and Event Hub configuration
SEARCH_ENDPOINT = "<your-search-service-endpoint>"  # e.g., https://<search-service-name>.search.windows.net
SEARCH_API_KEY = "<your-search-service-api-key>"
SEARCH_INDEX_NAME = "<your-index-name>"

def main(event: EventHubEvent):
    # Extract the message from Event Hub
    try:
        logging.info("Processing Event Hub message")
        event_data = event.get_body().decode('utf-8')
        message = json.loads(event_data)

        # Initialize Search Client
        search_client = SearchClient(endpoint=SEARCH_ENDPOINT, index_name=SEARCH_INDEX_NAME, credential=SEARCH_API_KEY)

        # Index the document
        logging.info(f"Adding document to Azure Search index: {message}")
        response = search_client.upload_documents(documents=[message])

        # Check the response for success
        if response[0].succeeded:
            logging.info("Document successfully added to the search index")
        else:
            logging.error(f"Failed to add document: {response[0].error_message}")

    except Exception as e:
        logging.error(f"Error processing message: {str(e)}")
