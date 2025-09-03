# 🎬 Async API сервис для фильмов

Учебный проект: ETL-процесс для переноса данных в Elasticsearch в нужном формате + FastAPI-сервис
для получения информации о фильмах, жанрах и персонах.

Так же реализованы функциональны тесты.

Проект запускается в Docker с помощью `docker-compose`.

В REVIEW_project написан ответ на первый таск.

Еще добавлен openapi.json, сделанный по информации из FastApi.

http://localhost:8000/api/openapi/
либо
http://localhost:8000/redoc

можно посмотреть на документцию, там все расписано, включая docstring.

---

## 🚀 Возможности

- **API на FastAPI**:
  - Получение списка фильмов с пагинацией и сортировкой
  - Детальная информация о фильме по UUID
  - Список жанров и детальная информация о жанре
  - Список персон и фильмы, в которых они участвовали
- **Документация**:
  - Swagger/OpenAPI доступен по адресу:  
    👉 `http://127.0.0.1:8000/api/openapi`
  - Так же на redoc:
    👉 `http://127.0.0.1:8000/redoc`
  - В репозитории есть openapi.json, со всей информацией по проекту.
- **Тестирование**:
  - Доступны функциональные тесты, покрывающие все эндпоинты.
  - Завернуты в docker compose

## ⚙️ Требования

- [Docker](https://docs.docker.com/get-docker/)  
- [Docker Compose](https://docs.docker.com/compose/install/)  

---

## ▶️ Запуск проекта

1. Клонируйте репозиторий:
   ```bash
   git clone https://github.com/NurzhanSarsenbayev/Async_API_sprint_2
   
2. Создайте .env файл на основе примера (.env-example).
   Надо создать .env для api (в корне проекта) и для test (в директории tests/)

3. Запустите проект:
    cd tests -> перейдите в папку tests
    docker compose up --build

4. После запуска сервисы будут доступны:

    FastAPI: http://127.0.0.1:8000/api/openapi
    Elasticsearch: http://127.0.0.1:9200

5. Для повторного включения тестов, перезапустите сервис tests в docker compose:
    Из директории tests :  docker compose up --build tests