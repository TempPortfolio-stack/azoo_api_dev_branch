import time
import traceback
from datetime import datetime

# from fastapi_pagination import paginate , Params
# from sqlalchemy.sql.expression import func
import boto3
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Model.schema import (
    ReturnResultSchema,
    IsConnectedSchema,
    ReturnMessageSchema,
)
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger

router = APIRouter(
    prefix="/cure",
)


@router.post(
    path="/is_connected",
    tags=["문서 개요 3.11 : /is_connected"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_is_connected(
    params: IsConnectedSchema, access_token=Depends(JWTBearer())
):
    """
    Admin 이 curedata 생성할때 팝업창에 입력한, DB 커넥션 정보가 유효한지 확인하는 API
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/is_connected"

        try:
            access_key = params.access_key
            secret_key = params.secret_key
            bucket_name = params.bucket_name

            if params.service_name == "NAVER":
                service_name = "s3"
                region_name = settings["REGION_NAME"]  # "kr-standard"
                endpoint_url = settings[
                    "ENDPOINT_URL"
                ]  # "https://kr.object.ncloudstorage.com"

            s3_resource = boto3.resource(
                service_name,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
            )
            s3_resource.Bucket(bucket_name) in s3_resource.buckets.all()
        except:
            is_connected = False
        else:
            is_connected = True
        finally:
            del s3_resource

        end = time.time()
        result = {
            "end_point": end_point,
            "result": is_connected,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(f"Result:{result} // Params:{params}")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:None \n {traceback.format_exc()}"
        )
        fastapi_logger.error(fail_message)

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
