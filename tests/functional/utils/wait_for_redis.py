import time
import redis
import os

if __name__ == '__main__':
    redis_host = os.getenv("REDIS_HOST", "redis")
    client = redis.Redis(host=redis_host, port=6379)
    while True:
        try:
            client.ping()
            print("✅ Redis is up!")
            break
        except redis.exceptions.ConnectionError:
            print("⏳ Waiting for Redis...")
            time.sleep(1)