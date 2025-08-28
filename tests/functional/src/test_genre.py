import pytest
import pytest_asyncio
from aiohttp import ClientSession

BASE_URL = "http://tests_api:8000/api/v1/genres/"

@pytest.mark.asyncio
async def test_genre_validation(http_session: ClientSession):
    # невалидный UUID
    async with http_session.get(f"{BASE_URL}123") as resp:
        assert resp.status == 422  # ошибка валидации

@pytest.mark.asyncio
async def test_get_genre_by_id(http_session: ClientSession):
    genre_id = "babf7031-6c46-4a02-aaf4-e3e17d948a82"   # Action
    async with http_session.get(f"{BASE_URL}{genre_id}") as resp:
        assert resp.status == 200
        data = await resp.json()

        assert "uuid" in data
        assert "name" in data
        assert data["uuid"] == genre_id


@pytest.mark.asyncio
async def test_get_all_genres(http_session: ClientSession):
    async with http_session.get(f"{BASE_URL}") as resp:
        assert resp.status == 200
        data = await resp.json()

        assert isinstance(data, list)
        assert len(data) > 0

        genre = data[0]
        assert "uuid" in genre
        assert "name" in genre


@pytest.mark.asyncio
async def test_genre_cache(redis_client, http_session: ClientSession):
    """Проверяем, что ответ кешируется в Redis"""
    genre_id = "babf7031-6c46-4a02-aaf4-e3e17d948a82"

    # Удалим ключ из редиса, чтобы проверить с нуля
    await redis_client.flushall()

    # Первый запрос — должен сходить в ES
    async with http_session.get(f"{BASE_URL}{genre_id}") as resp:
        assert resp.status == 200
        data = await resp.json()
        assert data["uuid"] == genre_id

    # Проверяем, что в кэше появился ключ
    keys = await redis_client.keys("*")
    assert len(keys) > 0

    # Второй запрос — данные должны вернуться из кэша
    async with http_session.get(f"{BASE_URL}{genre_id}") as resp2:
        assert resp2.status == 200
        data2 = await resp2.json()
        assert data2 == data  # ответ идентичен
