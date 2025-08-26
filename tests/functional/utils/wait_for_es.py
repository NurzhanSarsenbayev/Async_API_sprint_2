import os
import time
from elasticsearch import Elasticsearch

host = os.getenv("ES_HOST", "http://elasticsearch:9200")
es = Elasticsearch(hosts=host, verify_certs=False)

for i in range(120):
    try:
        if es.ping():
            print("✅ ES is ready")
            break
    except Exception as e:
        print("ES not ready yet:", repr(e))
    time.sleep(1)
else:
    raise RuntimeError("Elasticsearch didn't start in time")