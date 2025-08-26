import os
import time
import requests


host = os.getenv("API_HOST", "http://tests_api:8000")
health_endpoint = os.getenv("API_HEALTH_ENDPOINT", "/api/openapi")
url = f"{host}{health_endpoint}"

for i in range(120):
    try:
        resp = requests.get(url)
        if resp.status_code == 200:
            print("✅ API is ready")
            break
    except Exception as e:
        print("API not ready yet:", repr(e))
    time.sleep(1)
else:
    raise RuntimeError("API didn't start in time")