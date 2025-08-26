import pytest

@pytest.mark.asyncio
async def test_get_film_by_id(http_session):
    film_id = "8ce08b1f-d2b6-4d28-85f1-0096ea085b44"
    url = f"http://tests_api:8000/api/v1/films/{film_id}"

    async with http_session.get(url) as response:
        assert response.status == 200