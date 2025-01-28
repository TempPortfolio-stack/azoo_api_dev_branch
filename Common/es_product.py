import os

from pydantic import SecretStr
from pydantic_settings import BaseSettings

from Settings.Env.env_settings import settings


class ESAuth(BaseSettings):
    """Settings for Elasticsearch."""

    host: str = os.environ.get("ES_HOST", settings["ES_HOST"])
    user: str = os.environ.get("ES_USER", settings["ES_USER"])
    password: SecretStr = os.environ.get("ES_PASSWORD", settings["ES_PASSWORD"])


es_auth = ESAuth()

PRODUCTS_INDEX_NAME = "products"

PRODUCTS_INDEX_SYNONYMS = []

PRODUCTS_INDEX_SETTINGS = {
    "analysis": {
        "analyzer": {
            "product_index_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    "autocomplete_filter",
                ],
            },
            "product_search_analyzer": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": [
                    "lowercase",
                    # "autocomplete_filter",  # 여기에 autocomplete_filter 추가
                    # "synonym_filter",
                ],
            },
        },
        "filter": {
            # "synonym_filter": {
            #     "type": "synonym_graph",
            #     "expand": True,
            #     "lenient": True,
            #     "synonyms": PRODUCTS_INDEX_SYNONYMS,
            # },
            "autocomplete_filter": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20,
            },
        },
    },
}


PRODUCTS_INDEX_MAPPINGS = {
    "properties": {
        "id": {"type": "long"},
        "updated_at": {"type": "date"},
        "name": {
            "type": "text",
            "search_analyzer": "product_search_analyzer",
            "fields": {
                "ngrams": {
                    "type": "text",
                    "analyzer": "product_index_analyzer",
                    "search_analyzer": "product_search_analyzer",
                },
            },
        },
    }
}
