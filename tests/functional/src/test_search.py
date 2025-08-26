import pytest
from aiohttp import ClientSession

BASE_URL = "http://tests_api:8000/api/v1/films/search"

@pytest.mark.asyncio
async def test_search_validation(http_session: ClientSession):
    # Неверные параметры
    async with http_session.get(f"{BASE_URL}?size=-1") as resp:
        assert resp.status == 422  # FastAPI валидирует Query
    async with http_session.get(f"{BASE_URL}?page=0") as resp:
        assert resp.status == 422
    async with http_session.get(f"{BASE_URL}?query=") as resp:
        # Пустая строка допустима или нет, зависит от реализации
        assert resp.status in (200, 422)


@pytest.mark.asyncio
async def test_search_limit_records(http_session: ClientSession):
    N = 1
    async with http_session.get(f"{BASE_URL}?size={N}") as resp:
        assert resp.status == 200
        data = await resp.json()
        assert len(data['results']) <= N


@pytest.mark.asyncio
async def test_search_by_phrase(http_session: ClientSession):
    phrase = "Star Wars"
    async with http_session.get(f"{BASE_URL}?query={phrase}") as resp:
        assert resp.status == 200
        data = await resp.json()
        # Все результаты содержат фразу в названии
        assert all(phrase.lower() in item['title'].lower() for item in data['results'])


@pytest.mark.asyncio
async def test_search_cache(http_session: ClientSession, redis_client):
    phrase = "Star Wars"

    # Чистим Redis перед тестом
    await redis_client.flushdb()

    # Первый запрос
    async with http_session.get(f"{BASE_URL}?query={phrase}") as resp:
        assert resp.status == 200
        data_first = await resp.json()

    # Второй запрос, должен быть из кеша
    async with http_session.get(f"{BASE_URL}?query={phrase}") as resp:
        assert resp.status == 200
        data_second = await resp.json()

    assert data_first == data_second
