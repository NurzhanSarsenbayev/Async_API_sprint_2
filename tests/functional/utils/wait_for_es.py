# tests/functional/utils/wait_for_es.py
import time
from elasticsearch import Elasticsearch
import os

if __name__ == '__main__':
    es_host = os.getenv("ELASTIC_HOST", "http://elastic:9200")
    es = Elasticsearch(hosts=[es_host])
    while True:
        if es.ping():
            print("✅ Elasticsearch is up!")
            break
        print("⏳ Waiting for Elasticsearch...")
        time.sleep(1)