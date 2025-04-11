from elasticsearch import Elasticsearch
from typing import List
from tqdm import tqdm

from config import INDEX_NAME
from db import collection
from utils import get_es_client


def index_data(documents: List[dict]):
    es = get_es_client(max_retries=5, sleep_time=5)
    _ = _create_index(es=es, index_name=INDEX_NAME)
    _ = _insert_documents(es=es, index_name=INDEX_NAME, documents=documents)
    print(f'Indexed {len(documents)} documents into Elasticsearch index {INDEX_NAME}')

def _create_index(es: Elasticsearch, index_name: str) -> dict:
    es.indices.delete(index=index_name, ignore_unavailable=True)
    return es.indices.create(index=index_name)

def _insert_documents(es: Elasticsearch, index_name: str, documents: List[dict]) -> dict:
    operations = []
    for doc in tqdm(documents, total=len(documents), desc='Indexing documents'):
        operations.append({'index': {'_index': index_name}})
        operations.append(doc)
    return es.bulk(operations=operations)


if __name__ == "__main__":
    documents = collection.find()

    index_data(documents=documents)
