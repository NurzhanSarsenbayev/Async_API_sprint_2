import pytest
import pytest_asyncio
from aiohttp import ClientSession

BASE_URL = "http://tests_api:8000/api/v1/search"

@pytest.mark.asyncio
async def test_search_validation(http_session: ClientSession):
    # Некорректный размер
    async with http_session.get(f"{BASE_URL}?size=-1&query=test") as resp:
        assert resp.status == 422

    # Пустой query
    async with http_session.get(f"{BASE_URL}?query=") as resp:
        assert resp.status == 422

@pytest.mark.asyncio
async def test_search_limit_records(http_session: ClientSession):
    N = 2
    async with http_session.get(f"{BASE_URL}?query=test&size={N}") as resp:
        assert resp.status == 200
        data = await resp.json()
        # Проверяем длину списков
        assert len(data["films"]) <= N
        assert len(data["persons"]) <= N
        assert len(data["genres"]) <= N


@pytest.mark.asyncio
async def test_search_by_phrase(http_session: ClientSession):
    phrase = "Star Wars"
    async with http_session.get(f"{BASE_URL}?query={phrase}&page=1&size=5") as resp:
        assert resp.status == 200
        data = await resp.json()
        # Проверяем, что хотя бы один результат содержит фразу
        assert any(phrase.lower() in f["title"].lower() for f in data["films"]) or \
               any(phrase.lower() in p["full_name"].lower() for p in data["persons"]) or \
               any(phrase.lower() in g["name"].lower() for g in data["genres"])


@pytest.mark.asyncio
async def test_search_cache(http_session: ClientSession, redis_client):
    phrase = "Star Wars"

    # Чистим Redis перед тестом
    await redis_client.flushdb()

    # Первый запрос (кэш пуст)
    async with http_session.get(f"{BASE_URL}?query={phrase}&page=1&size=3") as resp:
        assert resp.status == 200
        data1 = await resp.json()

    # Второй запрос (должен отработать из кэша)
    async with http_session.get(f"{BASE_URL}?query={phrase}&page=1&size=3") as resp:
        assert resp.status == 200
        data2 = await resp.json()

    assert data1 == data2  # Результаты должны совпадать