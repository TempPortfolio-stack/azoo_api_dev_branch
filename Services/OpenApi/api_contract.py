import json
import os
import time
import traceback
from datetime import datetime

import aiohttp
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Model.schema import MakeContractSchema, ReturnResultSchema, ReturnResultDictSchema
from Settings.Database.database import async_get_db
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import openapi_logger

router = APIRouter(
    prefix="/openapi",
)


@router.post(
    path="/generate_contract_url",
    tags=["OpenAPI"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultDictSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        460: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_generate_contract_url(
    params: MakeContractSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    글로싸인의 OpenAPI 를 이용한 전자계약서 발행

    - contract_name \n
    - common_message \n
    - lang = 'ko' / 'eng' \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/generate_contract_url"

        # if params.lang == "ko":
        #     template_id = settings["GLOSIGN_KO_TEMPLATE_ID"]
        # else:
        #     template_id = settings["GLOSIGN_ENG_TEMPLATE_ID"]

        template_id = settings["GLOSIGN_ENG_TEMPLATE_ID"]

        data = {
            "templateId": template_id,  # 부동산 매매계약서
            "contractName": params.contract_name,  # 계약명
            "commonMessage": params.common_message,
            "sendLang": "ko",  # params.lang , # 발송 언어  무조건 ko로 해야함
            "certLang": "ko",  # params.lang , # 인증서 언어	 무조건 ko로 해야함
            "clientId": settings["GLOSIGN_CLIENT_ID"],
        }

        url = settings["GLOSIGN_MAKE_CONTRACT_URL"]
        headers = {
            "Authorization": f"Bearer {settings['GLOSIGN_ACCESS_TOKEN_API_KEY']}"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:  # 요청 성공
                    res = await response.json()

                    result_data = res["link"]
                    contract_unique_id = res["link_id"]
                    status_code = 200
                    openapi_logger.info(
                        f"글로싸인 계약서 생성 API 성공 : {json.dumps(res , indent=4 , ensure_ascii=False)}"
                    )

                else:  # 요청 실패
                    openapi_logger.error(
                        f"글로싸인 계약서 생성 API 실패 상태 코드: {response.status}"
                    )
                    raise Exception(
                        f"글로싸인 계약서 생성 API 호출 실패 상태 코드: {response.status}"
                    )
        end = time.time()

        result = {
            "end_point": end_point,
            "result": {
                "contract_url": result_data,
                "contract_unique_id": contract_unique_id,
            },
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


@router.post(
    path="/send_contract_email",
    tags=["OpenAPI"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultDictSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        460: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_send_contract_email(
    params: MakeContractSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    글로싸인의 OpenAPI 를 이용한 전자계약서 발행
    class MakeContractSchema(BaseModel):    \n
        contract_name:str = Query(... , description="계약서 이름") \n
        common_message:str = Query(... , description="메시지") \n
        lang:str = Query(... , description="발송언어--   'ko' / 'eng' ") \n
        user_name:str = Query(default="", description="user name") \n
        user_email:str|None = Query(default=None , description="user email") \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/send_contract_email"

        # if params.lang == "ko":
        #     template_id = settings["GLOSIGN_KO_TEMPLATE_ID"]
        # else:
        #     template_id = settings["GLOSIGN_ENG_TEMPLATE_ID"]
        template_id = settings["GLOSIGN_ENG_TEMPLATE_ID"]

        data = {
            "templateId": template_id,
            "contractName": params.contract_name,  # $$ 공통 계약명 (string)
            "commonMessage": params.common_message,
            "commonExp": "10",
            "lang": "kr",
            "certLang": "kr",
            # "mailId": "",
            "emailFlag": True,  # 이메일 전송여부
            "mobileFlag": False,  # SMS/Kakao 전송 여부
            "clientId": settings["GLOSIGN_CLIENT_ID"],
            "contractList": [  # 계약 리스트
                {
                    "signOrder": False,  # $$ 서명 순서설정
                    "isReview": False,
                    "receiverList": [  # 서명자 리스트
                        {
                            "signOrderNumber": "1",  # $$ 계약자 순번 ( 1부터 시작 )
                            "name": params.user_name,  # $$ 계약자 이름 (string)
                            "email": params.user_email,  # $$ 참여자 이메일 (string)
                            # "userPhone": "01077395518",
                            # "userPhoneCode": "82",
                            "lang": "kr",
                            "expired_day": 10,  # 만료일 (number)
                        },
                    ],
                }
            ],
        }

        url = settings["GLOSIGN_EMAIL_CONTRACT_URL"]
        headers = {
            "Authorization": f"Bearer {settings['GLOSIGN_ACCESS_TOKEN_API_KEY']}"
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                if response.status == 200:  # 요청 성공
                    res = await response.json()

                    value = res["contractList"][0]
                    contractId = value["contractId"]
                    contract_url = value["receiverList"][0]["url"]

                    status_code = 200
                    openapi_logger.info(
                        f"글로싸인 계약서 생성 API 성공 : {json.dumps(res , indent=4 , ensure_ascii=False)}"
                    )

                else:  # 요청 실패
                    openapi_logger.error(
                        f"글로싸인 계약서 생성 API 실패 상태 코드: {response.status}"
                    )
                    raise Exception(
                        f"글로싸인 계약서 생성 API 호출 실패 상태 코드: {response.status}"
                    )
        end = time.time()

        result = {
            "end_point": end_point,
            "result": {"contract_url": contract_url, "contract_unique_id": contractId},
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


@router.get(
    path="/contract_pdf",
    tags=["OpenAPI"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultDictSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        460: {"description": "No contents", "model": ReturnResultSchema},
    },
)
async def api_get_contract_pdf(
    contract_unique_id: str = "", access_token=Depends(JWTBearer())
):
    """
    contract_unique_id = "c2b888d185bd2caba6a5d267f8e0ba58b" \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/contract_pdf"

        if os.path.exists(
            f"/mnt/nfs/azoo/{settings['DEPLOY_MODE']}/contracts/{contract_unique_id}.pdf"
        ):
            # pdf_url = f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/files/dev/contracts/{contract_unique_id}.pdf"
            pdf_url = f"https://dev.azoo.ai/azoo_fastapi/files/{settings['DEPLOY_MODE']}/contracts/{contract_unique_id}.pdf"
        else:
            pdf_url = f"{contract_unique_id}.pdf 는 존재하지 않습니다."
            raise Exception(
                f"contract_unique_id : {contract_unique_id}.pdf 는 NAS에 존재하지 않습니다."
            )

        end = time.time()

        result = {
            "end_point": end_point,
            "result": {"pdf_download_url": pdf_url},
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }

        openapi_logger.info(
            f"Result:{result} // contract_unique_id:{contract_unique_id}"
        )
        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = f"Endpoint:{end_point} \n contract_unique_id:{contract_unique_id} \n {traceback.format_exc()}"
        openapi_logger.error(fail_message)
        # error_message = str(e) + '\n' + error_traceback # 에러 메시지와 traceback 결합
        # truncated_error = error_message[:300] # 문자열을 300글자로 자름
        result = {
            "end_point": end_point,
            "result": pdf_url,
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
