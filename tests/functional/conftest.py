# functional/conftest.py
import asyncio
import json
import os
import redis.asyncio
import pytest_asyncio
import aiohttp
from elasticsearch import AsyncElasticsearch, helpers

from functional.testdata.es_mapping import MOVIES_MAPPING, MOVIES_INDEX


# ---------- aiohttp session ----------
@pytest_asyncio.fixture
async def http_session():
    session = aiohttp.ClientSession()
    yield session
    await session.close()


# ---------- elasticsearch client ----------
@pytest_asyncio.fixture(scope="session")
async def es_client():
    client = AsyncElasticsearch(hosts=["http://elasticsearch:9200"])
    yield client
    await client.close()

@pytest_asyncio.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture()
async def redis_client(event_loop):
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", 6379))
    client = await redis.asyncio.from_url(f"redis://{host}:{port}", decode_responses=True)
    yield client
    await client.aclose()

# ---------- prepare elasticsearch with data ----------
@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_es(es_client):
    # пересоздаём индекс
    if await es_client.indices.exists(index=MOVIES_INDEX):
        await es_client.indices.delete(index=MOVIES_INDEX)

    await es_client.indices.create(index=MOVIES_INDEX, body=MOVIES_MAPPING)

    actions = []
    with open("functional/testdata/test_data.json", "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            doc = json.loads(line)

            # поддерживаем оба варианта: экспорт из ES и "простой" json
            doc_id = doc.get("_id") or doc.get("id") or doc.get("uuid")
            source = doc.get("_source") or doc

            if not doc_id:
                raise ValueError(f"❌ Не найден ID в документе: {doc}")

            actions.append({
                "_index": MOVIES_INDEX,
                "_id": doc_id,
                "_source": source,
            })

    # bulk insert
    if actions:
        success, errors = await helpers.async_bulk(
            es_client, actions, raise_on_error=False
        )

    # refresh
    await es_client.indices.refresh(index=MOVIES_INDEX)

    # проверка загрузки
    count = await es_client.count(index=MOVIES_INDEX)
    assert count["count"] > 0, "❌ Данные не загрузились в Elasticsearch"


