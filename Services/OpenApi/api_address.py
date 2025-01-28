import json
import time
import traceback
from datetime import datetime

import aiohttp
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Model.schema import AddressCheckSchema, ReturnResultSchema
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import openapi_logger

router = APIRouter(
    prefix="/openapi",
)


@router.post(
    path="/address_checker",
    tags=["OpenAPI"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        460: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_noti_task_done(
    params: AddressCheckSchema, access_token=Depends(JWTBearer())
):
    """
    네이버의 OpenAPI 를 이용한 주소 검색
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/address_checker"

        url = settings["NAVER_ROAD_API_URL"]
        clien_id = settings["NAVER_Client_ID"]
        client_secret = settings["NAVER_Client_Secret"]

        headers = {
            "X-NCP-APIGW-API-KEY-ID": f"{clien_id}",
            "X-NCP-APIGW-API-KEY": f"{client_secret}",
        }
        req_params = {"query": params.target_address}

        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=req_params) as response:
                if response.status == 200:  # 요청 성공
                    res = await response.json()
                    try:
                        result_data = res["addresses"][0]
                        status_code = 200
                        openapi_logger.info(
                            f"네이버 주소검색 API 성공 : {json.dumps(result_data , indent=4 , ensure_ascii=False)}"
                        )
                    except:
                        result_data = []
                        status_code = 460
                        openapi_logger.warning(
                            f"네이버 주소검색 API 경고 : 주소 정보 존재하지 않음"
                        )

                else:  # 요청 실패
                    openapi_logger.error(
                        f"네이버 주소검색 API 실패 상태 코드: {response.status}"
                    )
                    raise Exception(
                        f"네이버 주소검색 API 상태코드 이상: {response.status}"
                    )
        end = time.time()

        result = {
            "end_point": end_point,
            "result": result_data,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        openapi_logger.info(f"Result:{result} // Params:{params}")
        res = JSONResponse(content=jsonable_encoder(result), status_code=status_code)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{params} \n {traceback.format_exc()}"
        )
        openapi_logger.error(fail_message)
        # error_message = str(e) + '\n' + error_traceback # 에러 메시지와 traceback 결합
        # truncated_error = error_message[:300] # 문자열을 300글자로 자름
        result = {
            "end_point": end_point,
            "result": "Error",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
