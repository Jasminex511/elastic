from elasticsearch import Elasticsearch
from typing import List

from config import INDEX_NAME
from db import collection
from utils import get_es_client


def index_data(documents: List[dict]):
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = _create_index(es=es, index_name=INDEX_NAME)
    _ = _insert_documents(es=es, index_name=INDEX_NAME, documents=documents)
    print('Indexing completed!')

def _create_index(es: Elasticsearch, index_name: str) -> dict:
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(index=index_name)

def _insert_documents(es: Elasticsearch, index_name: str, documents: List[dict]) -> dict:
    operations = []
    for doc in documents:
        doc["_id"] = str(doc["_id"])
        operations.append({'index': {'_index': index_name}})
        operations.append(doc)
    print(f'Indexing {len(operations)} documents into Elasticsearch index {INDEX_NAME}')
    return es.bulk(operations=operations)


if __name__ == "__main__":
    documents = collection.find()

    index_data(documents=documents)
