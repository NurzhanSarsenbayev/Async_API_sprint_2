import os

from fastapi import APIRouter, Depends, Query

from services.films import FilmService, get_film_service
from services.persons import PersonService, get_person_service
from services.genres import GenreService, get_genre_service
from models.search import SearchResults
from redis.asyncio import Redis
  # функция Depends для Redis

router = APIRouter()


# Подключение к Redis через Depends
def get_redis() -> Redis:
    host = os.getenv("REDIS_HOST", "redis")
    port = int(os.getenv("REDIS_PORT", 6379))
    return Redis(host=host, port=port, decode_responses=True)

@router.get("/", response_model=SearchResults)
async def global_search(
    query: str = Query(..., min_length=1, description="Поисковая строка для всех сущностей"),
    page: int = Query(1, ge=1, description="Номер страницы"),
    size: int = Query(10, ge=1, description="Количество элементов на странице"),
    film_service: FilmService = Depends(get_film_service),
    person_service: PersonService = Depends(get_person_service),
    genre_service: GenreService = Depends(get_genre_service),
    redis_client: Redis = Depends(get_redis)
):
    """
    Глобальный поиск по фильмам, персонам и жанрам с пагинацией и кэшем Redis.
    """
    cache_key = f"search:{query}:{page}:{size}"
    cached = await redis_client.get(cache_key)
    if cached:
        return SearchResults.parse_raw(cached)

    # Вызываем сервисы поиска
    films = await film_service.search_films(query_str=query, size=size)
    persons = await person_service.search_persons(query_str=query)
    genres = await genre_service.search_genres(query_str=query)

    result = SearchResults(films=films, persons=persons, genres=genres)

    # Кэшируем на 5 минут
    await redis_client.set(cache_key, result.json(), ex=60*5)

    return result