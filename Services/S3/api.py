import time
import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# from sqlalchemy.sql.expression import func
from sqlalchemy.sql.expression import select

from Common.jwt import JWTBearer
from Common.s3_manager import S3Manager
from Common.util import generate_uuid, generate_uuid7
from Model.new_models import ProductWarehouseModel, ProductBaseInfoModel
from Model.schema import (
    ReturnResultDictSchema,
    S3NotiUploadDoneSchema,
    NasReqOriginalDatasetZip,
    ReturnMessageSchema,
    ReturnResultSchema,
    ReturnResultListSchema,
)
from Services.S3.tasks_of_nas_makezip import task_nas_makezip
from Services.S3.tasks_of_s3_download import task_s3_download
from Settings.Database.database import async_get_db
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger as logger

router = APIRouter(
    prefix="/s3",
)


@router.get(
    path="/generate_original_dataset_id",
    tags=["문서 개요 3.20 /generate_original_dataset_id"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_generate_original_dataset_id(access_token=Depends(JWTBearer())):
    """
    generate new UUID for dataset_id \n
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/generate_original_dataset_id"

        new_dataset_id = generate_uuid(f"{settings['DEPLOY_MODE']}")

        end = time.time()
        result = {
            "end_point": end_point,
            "result": {"dataset_id": new_dataset_id},
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // Params:None")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


@router.get(
    path="/s3_upload_url",
    tags=["문서 개요 3.4 /s3_upload_url"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultDictSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_s3_upload_url(
    dataset_id: str = "",
    type: str = "",
    source_file_name: str = "",
    access_token=Depends(JWTBearer()),
):
    """
    dataset_id:UUID   \n
    type:str = "sample" / "report_html" / "original_dataset" / "thumbnail" / "company_logo" \n
    source_file_name:str    ex)a.png  , b.pdf   , c.txt   ,  d.csv  \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/api_s3_upload_url"

        s3_manager = S3Manager()
        if type == "thumbnail":
            key = f"{settings['DEPLOY_MODE']}/{dataset_id}/{type}/thumbnail.jpg"
        elif type == "company_logo":
            key = f"{settings['DEPLOY_MODE']}/{dataset_id}/{type}/company_logo.jpg"
        else:
            key = f"{settings['DEPLOY_MODE']}/{dataset_id}/{type}/{source_file_name}"

        upload_url = s3_manager.create_upload_url(key=key, exp_sec=600)

        end = time.time()
        result = {
            "end_point": end_point,
            "result": {"upload_url": upload_url},
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // Req Params--> dataset_id:{dataset_id} , type:{type} , source_file_name:{source_file_name}")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager


@router.post(
    path="/s3_noti_upload_done",
    tags=["문서 개요 3.5 /s3_noti_upload_done"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_post_s3_noti_upload_done(
    data: S3NotiUploadDoneSchema, access_token=Depends(JWTBearer())
):
    """
    dataset_id:UUID \n
    type:str = "sample" / "report_html" / "original_dataset" / "thumbnail" \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/s3_noti_upload_done"

        if (
            data.type == "sample"
            or data.type == "report_html"
            or data.type == "thumbnail"
            or data.type == "company_logo"
        ):
            s3_manager = S3Manager()
            key_prefix = f"{settings['DEPLOY_MODE']}/{data.dataset_id}/{data.type}"
            value = s3_manager.set_public_to_object(key_prefix=key_prefix)
        elif data.type == "original_dataset":
            ### 다운로드 워커 시작!
            req_info_dict = {"dataset_id": data.dataset_id, "type": data.type}

            generated_task_id = str(generate_uuid7())
            task_id = task_s3_download.apply_async(
                args=[req_info_dict],
                queue="s3_dataset_download_queue",
                task_id=generated_task_id,
                countdown=0.05,
            )
            # value = task_id.get() # 이 부분에서 대기!
            # if value is None:
            #     raise Exception("에러 발생")
            logger.info(
                f"dataset_id: {data.dataset_id} download task is pushed into queue!"
            )
            # value = json.loads(value.decode("UTF-8"))
            pass

        end = time.time()
        result = {
            "end_point": end_point,
            "result": "Task has started!",
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // Params:None")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager


@router.post(
    path="/request_zip_original_dataset",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_post_request_zip_original_dataset(
    data: NasReqOriginalDatasetZip, access_token=Depends(JWTBearer())
):
    """
    class NasReqOriginalDatasetZip(BaseModel):  \n
        dataset_id:str = Query(...,description="UUID")     \n
        zip_type:str = Query(...,description="sample , original_dataset")  \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/request_zip_original_dataset"

        # make_zip task 호출함
        req_to_zip_worker = {
            "dataset_id": data.dataset_id,
            "zip_type": "original_dataset_zip",  # zip_type 은 "original_dataset_zip" , "azoo_product_zip" 이 있다.
        }

        generated_task_id = str(generate_uuid7())
        task_id = task_nas_makezip.apply_async(
            args=[req_to_zip_worker],
            queue="nas_makezip_queue",
            task_id=generated_task_id,
            countdown=0.05,
        )
        logger.info(
            f"original_dataset_id: {data.dataset_id} 압축 task is pushed into queue!"
        )

        end = time.time()
        result = {
            "end_point": end_point,
            "result": "Task has started!",
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        logger.info(f"Result:{result} // Params:None")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager


@router.get(
    path="/s3_access_url",
    tags=["문서 개요 3.24 /s3_access_url"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultListSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
        460: {
            "description": "sample 과 report html 외의 것을 요구함",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_s3_access_url(
    dataset_id: str = "", type: str = "", access_token=Depends(JWTBearer())
):
    """
    sample 과 report html 파일에 대한 public 접근 주소를 제공한다. \n
    dataset_id:UUID   \n
    type:str = "sample" / "report_html"  / "thumbnail"  \n
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/s3_access_url"

        if (
            type == "sample"
            or type == "report_html"
            or type == "thumbnail"
            or type == "company_logo"
        ):
            s3_manager = S3Manager()
            key_prefix = f"{settings['DEPLOY_MODE']}/{dataset_id}/{type}"
            object_list = s3_manager.get_s3_access_url(key_prefix=key_prefix)
            end = time.time()
            result = {
                "end_point": end_point,
                "result": object_list,
                "working_time_sec": end - begin,
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        else:
            end = time.time()
            result = {
                "end_point": end_point,
                "msg": "Sample , report_html 만 허용됩니다.",
                "working_time_sec": end - begin,
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=460)

        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager


@router.get(
    path="/azoo_product_download_url",
    tags=["문서 개요 3.24 /s3_access_url"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultListSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
        460: {
            "description": "sample 과 report html 외의 것을 요구함",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_azoo_product_download_url(
    purchase_id: int = 0,
    azoo_product_id: int = 0,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    purchase_id 를 넘겨주면, 해당 azoo_product 의 download url을 생성해준다.(다운로드 유효기간 60*60 초 = 1시간)
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/azoo_product_download_url"

        async with db.begin():  ### 트랜젝션
            ##### 여기서, product_warehouse 테이블에서 status 가 azoo_product 인것과 uploading 인것을 가져온다.
            result = await db.execute(
                select(ProductWarehouseModel.s3_key).where(
                    (ProductWarehouseModel.purchase_id == purchase_id),
                    (ProductWarehouseModel.product_id == azoo_product_id),
                )
            )

            s3_key = result.scalar()
            if s3_key is None:
                raise Exception(
                    f" purchase_id : {purchase_id} 의 s3_key 가 존재하지 않습니다."
                )

            logger.info(f"s3_key : {s3_key}")
            pass

        s3_manager = S3Manager()
        download_url = s3_manager.create_download_url(key=s3_key, exp_sec=3600)
        end = time.time()

        result = {
            "end_point": end_point,
            "result": download_url,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager


@router.get(
    path="/research_download_url",
    tags=["문서 개요 3.24 /s3_access_url"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultListSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
        460: {
            "description": "sample 과 report html 외의 것을 요구함",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_get_research_download_url(
    product_base_info_id: int = 0,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    product_base_info_id 를 넘겨주면, 해당되는 research dataset 의 download url을 생성해준다.(다운로드 유효기간 60*60 초 = 1시간)
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/research_download_url"

        logger.info(
            f"end_point:{end_point}로 요청이 들어왔습니다. ---> product_base_info_id:{product_base_info_id}"
        )
        async with db.begin():  ### 트랜젝션
            ##### 여기서, product_warehouse 테이블에서 status 가 azoo_product 인것과 uploading 인것을 가져온다.
            result = await db.execute(
                select(ProductBaseInfoModel.research_dataset_s3_key).where(
                    (ProductBaseInfoModel.id == product_base_info_id)
                )
            )

            s3_key = result.scalar()
            if s3_key is None:
                raise Exception(
                    f" product_base_info_id : {product_base_info_id} 의 s3_key 가 존재하지 않습니다."
                )

            logger.info(f"s3_key : {s3_key}")
            pass

        s3_manager = S3Manager()
        download_url = s3_manager.create_download_url(key=s3_key, exp_sec=3600)
        end = time.time()

        result = {
            "end_point": end_point,
            "result": download_url,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        logger.error(traceback.format_exc())
        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
    finally:
        if "s3_manager" in locals():
            del s3_manager
