import pytest
import pytest_asyncio
from aiohttp import ClientSession

BASE_URL = "http://tests_api:8000/api/v1/persons"

# -------------------- Тест на валидацию --------------------
@pytest.mark.asyncio
async def test_person_validation(http_session: ClientSession):

    async with http_session.get(f"{BASE_URL}/123") as resp:
        assert resp.status == 422  # неправильный формат UUID

# -------------------- Получение конкретного человека --------------------
@pytest.mark.asyncio
async def test_get_person_by_id(http_session: ClientSession):
    person_id = "15af8ae2-1b30-41f1-9573-824d12dd70cb"

    async with http_session.get(f"{BASE_URL}/{person_id}") as resp:
        assert resp.status == 200
        data = await resp.json()
        assert "uuid" in data
        assert data["uuid"] == person_id
        assert "full_name" in data
        assert isinstance(data["full_name"], str)

# -------------------- Получение всех людей --------------------
@pytest.mark.asyncio
async def test_get_all_persons(http_session: ClientSession):
    async with http_session.get(f"{BASE_URL}?page=1&size=5") as resp:
        assert resp.status == 200
        data = await resp.json()
        assert isinstance(data, list)
        assert all("uuid" in p and "full_name" in p for p in data)

# -------------------- Поиск всех фильмов с участием человека --------------------
@pytest.mark.asyncio
async def test_person_films(http_session: ClientSession):
    person_id = "15af8ae2-1b30-41f1-9573-824d12dd70cb"

    async with http_session.get(f"{BASE_URL}/{person_id}") as resp:
        assert resp.status == 200
        data = await resp.json()
        # Проверяем обязательные поля у персоны
        assert isinstance(data, dict)
        assert "uuid" in data
        assert "full_name" in data
        assert "role" in data
        assert "films" in data

        # Проверяем что films — это список словарей
        assert isinstance(data["films"], list)
        for film in data["films"]:
            assert isinstance(film, dict)
            assert "uuid" in film
            assert isinstance(film["uuid"], str)
            assert "title" in film
            assert isinstance(film["title"], str)
            assert "imdb_rating" in film
            assert isinstance(film["imdb_rating"], (float, int))  # может быть None, если рейтинг не указан

# -------------------- Поиск с учётом кэша в Redis --------------------
@pytest.mark.asyncio
async def test_person_cache(http_session: ClientSession, redis_client):
    person_name = "George Lucas"

    # Чистим Redis перед тестом
    await redis_client.flushdb()

    # Первый запрос (кэш пуст)
    async with http_session.get(f"{BASE_URL}?query={person_name}&page=1&size=3") as resp:
        assert resp.status == 200
        data1 = await resp.json()

    # Второй запрос (должен отработать из кэша)
    async with http_session.get(f"{BASE_URL}?query={person_name}&page=1&size=3") as resp:
        assert resp.status == 200
        data2 = await resp.json()

    assert data1 == data2  # Результаты должны совпадать
