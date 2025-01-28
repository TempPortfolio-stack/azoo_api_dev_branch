import time
import traceback

from elasticsearch import Elasticsearch
from fastapi import APIRouter, Depends, HTTPException, Query

from Common.es_product import es_auth, PRODUCTS_INDEX_NAME
from Model.schema import (
    ReturnResultSchema,
    ReturnProductListSchema,
    EsProductSchema,
)
from Settings.Logger.logging_config import fastapi_logger as logger

router = APIRouter(
    prefix="/search",
)


def get_es_client():
    """Get the dependency for ES client."""
    es_client = Elasticsearch(
        es_auth.host,
        basic_auth=(es_auth.user, es_auth.password.get_secret_value()),
    )
    return es_client


@router.get(
    path="/products/",
    tags=["ES_Search"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnProductListSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        500: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_search_products_get(
    query: str = Query(alias="q"), es_client: Elasticsearch = Depends(get_es_client)
):
    """
    엘라스틱서치에 저장된 product 정보를 넘겨주는 api
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/products/"
        logger.info(f"{end_point} -- GET 으로 요청이 들어왔습니다. : query:{query}")

        if len(query.strip()) == 0:
            raise HTTPException(
                status_code=400,
                detail="Please provide a valid query",
            )

        search_query = {
            "multi_match": {
                "query": query,
                "type": "most_fields",
                "operator": "and",
                "fields": ["name^3", "name.ngrams"],
            }
        }

        results = es_client.search(
            index=PRODUCTS_INDEX_NAME,
            query=search_query,
        )

        posts_found: list[EsProductSchema] = [
            EsProductSchema(**hit["_source"]) for hit in results["hits"]["hits"]
        ]

        logger.info(f"{end_point} -- GET query:{query} 반환값 --> {posts_found} ")
        return posts_found

    except Exception as e:
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
    finally:
        es_client.close()


@router.post(
    path="/products",
    tags=["ES_Search"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        500: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_search_products_post(
    product: EsProductSchema, es_client: Elasticsearch = Depends(get_es_client)
):
    """ """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/products"
        logger.info(
            f"{end_point} -- POST 으로 요청이 들어왔습니다. : product:{product}"
        )

        es_actions = []

        action = {"index": {"_index": PRODUCTS_INDEX_NAME, "_id": product.id}}
        es_actions.append(action)

        es_actions.append(product.dict())

        es_client.bulk(
            index=PRODUCTS_INDEX_NAME,
            operations=es_actions,
            filter_path="took,errors",
        )
        logger.info(
            f"{end_point} -- POST 으로 요청이 완료되었습니다. : product:{product}"
        )
        return product
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e),
        )
    finally:
        es_client.close()
