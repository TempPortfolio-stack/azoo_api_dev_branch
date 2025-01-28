# uvicorn main:app --reload --host=0.0.0.0 --port 61234
# 신규서버 ssh 터널링으로 접속: ssh -L 61234:192.168.1.196:51080 -p 19422 ml@211.118.0.177
# ML서버의 rabbitmq 상태 확인용 터널링: ssh -L 61234:211.45.184.102:15672 -p 19422 ml@211.118.0.177

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware

from Common.es_product import (
    PRODUCTS_INDEX_NAME,
    PRODUCTS_INDEX_SETTINGS,
    PRODUCTS_INDEX_MAPPINGS,
)
from Services.Auth.api import router as auth_router
from Services.Core.api_azoo_product import router as azoo_product_router
from Services.Core.api_package import router as core_packaging_router
from Services.Core.api_random_sampling import router as core_sampling_router
from Services.Core.api_send_email import router as send_email_router
from Services.Core.api_update import router as core_update_router
from Services.CureData.api_is_connected import router as is_connected_router
from Services.CureData.api_new_task import router as new_task_router
from Services.CureData.api_noti_task_done import router as noti_task_done_router
from Services.CureData.api_tasks_list_async import router as tasks_list_router
from Services.OpenApi.api_address import router as naver_address_router
from Services.OpenApi.api_contract import router as glosign_contract_router
from Services.Report.api_report import router as report_router
from Services.S3.api import router as s3_api_router
from Services.Search.api_search_product import get_es_client
from Services.Search.api_search_product import router as es_search_router
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger

# from Common.middleware import (CORSMiddleware, auth_middleware, csp_middleware,
#                                x_content_type_options_middleware,
#                                x_xss_protection_middleware)

SECRET_KEY = settings["SECRETKEY"]

# app = FastAPI(lifespan=lifespan , version='0.1.0' , description="AZOO FastApi" , root_path='/azoo_fastapi' , docs_url = '/docs' , redoc_url = "/redoc" , openapi_url="/openapi.json")
app = FastAPI(
    version="0.1.0",
    description="AZOO FastApi",
    root_path="/azoo_fastapi",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
)


origins = [
    "*",  # 또는 "http://localhost:5173"
]


# CORS 미들웨어 등록
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY)  # 세션 미들웨어


@app.get("/")
def hello():
    fastapi_logger.info("hoho")
    return {"message": "hello world"}


es_client = get_es_client()
# es_client.options(ignore_status=404).indices.delete(index=PRODUCTS_INDEX_NAME)
# fastapi_logger.info("Index `%s` is deleted if existing.", PRODUCTS_INDEX_NAME)

if not es_client.indices.exists(index=PRODUCTS_INDEX_NAME):
    es_client.indices.create(
        index=PRODUCTS_INDEX_NAME,
        settings=PRODUCTS_INDEX_SETTINGS,
        mappings=PRODUCTS_INDEX_MAPPINGS,
    )
    fastapi_logger.info("Index `%s` is created.", PRODUCTS_INDEX_NAME)


### Auth
app.include_router(auth_router)


### ZOODATA
app.include_router(tasks_list_router)
app.include_router(is_connected_router)
app.include_router(new_task_router)
app.include_router(noti_task_done_router)
app.include_router(naver_address_router)
app.include_router(glosign_contract_router)
app.include_router(s3_api_router)
app.include_router(core_update_router)
app.include_router(core_sampling_router)
app.include_router(core_packaging_router)
app.include_router(azoo_product_router)
app.include_router(send_email_router)
app.include_router(es_search_router)
app.include_router(report_router)

# 018f7542-f329-736a-a6c1-b24a8d25dbdc
# 018f7545-0674-7d23-98cc-a07c78c434c5
