from pydantic import Field
from pydantic_settings import BaseSettings


class TestSettings(BaseSettings):
    es_host: str = Field('http://localhost:9200', env='ELASTIC_HOST')
    es_index: str = Field('movies', env='ELASTIC_INDEX')
    #es_id_field: str = Field('uuid',  env='ELASTIC_ID_FIELD')
    #es_index_mapping: dict = Field(..., env='ELASTIC_INDEX_MAPPING')

    redis_host: str = Field('localhost', env='REDIS_HOST')
    service_url: str = Field('http://localhost:8000', env='SERVICE_URL')

test_settings = TestSettings()