import os
import time
import redis

host = os.getenv("REDIS_HOST", "redis")
port = int(os.getenv("REDIS_PORT", "6379"))

for i in range(120):
    try:
        r = redis.Redis(host=host, port=port, decode_responses=True)
        r.ping()
        print("✅ Redis is ready")
        break
    except Exception as e:
        print("Redis not ready yet:", repr(e))
    time.sleep(1)
else:
    raise RuntimeError("Redis didn't start in time")