import pytest
import pytest_asyncio
from aiohttp import ClientSession


BASE_URL = "http://tests_api:8000/api/v1/films"

@pytest.mark.asyncio
async def test_film_validation(http_session: ClientSession):
    # Некорректный UUID
    async with http_session.get(f"{BASE_URL}/123") as resp:
        assert resp.status == 422

@pytest.mark.asyncio
async def test_get_film_by_id(http_session: ClientSession):
    film_id = "900e93d9-21f2-4c62-b8d2-32de32110a16"  # пример существующего UUID
    async with http_session.get(f"{BASE_URL}/{film_id}") as resp:
        assert resp.status == 200
        data = await resp.json()
        assert data["uuid"] == film_id

@pytest.mark.asyncio
async def test_get_all_films(http_session: ClientSession):
    async with http_session.get(f"{BASE_URL}?page=1&size=10") as resp:
        assert resp.status == 200
        data = await resp.json()
        # проверяем, что это список и длина <= 10
        assert isinstance(data, list)
        assert len(data) <= 10
        # можно проверить наличие нужных полей у первого фильма
        if data:
            assert "title" in data[0]
            assert "uuid" in data[0]

@pytest.mark.asyncio
async def test_film_cache(http_session: ClientSession, redis_client):
    film_id = "900e93d9-21f2-4c62-b8d2-32de32110a16"

    # Чистим Redis перед тестом
    await redis_client.flushdb()

    # Первый запрос (кэш пуст)
    async with http_session.get(f"{BASE_URL}/{film_id}") as resp:
        assert resp.status == 200

    # Проверяем, что данные появились в Redis
    keys = await redis_client.keys("*")
    assert len(keys) > 0

    # Второй запрос (должен использовать кэш)
    async with http_session.get(f"{BASE_URL}/{film_id}") as resp:
        assert resp.status == 200
