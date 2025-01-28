import time
import traceback
from datetime import datetime

from fastapi import APIRouter, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Model.schema import LoginSchema, ReturnResultSchema
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/auth",
)


@router.post(
    path="/login",
    tags=["로그인- Auth"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
        401: {"description": "username is not valid", "model": ReturnResultSchema},
    },
)
async def api_login(params: LoginSchema):
    """
    username 으로 login을 하면, access token을 반환함
    """
    try:
        begin = time.time()
        end_point = f"{router.prefix}/login"

        if params.user != settings["ALLOWED_USER"]:
            raise HTTPException(status_code=401, detail="username is not valid")

        jwt_token = JWTBearer.create_jwt(params.user)
        end = time.time()

        result = {
            "end_point": end_point,
            "access_token": jwt_token,
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(f"Result:{result} // Params:{params}")

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except HTTPException:
        result = {
            "end_point": end_point,
            "result": "username is not valid",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=401)
        return res

    except Exception:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{params} \n {traceback.format_exc()}"
        )
        fastapi_logger.error(fail_message)

        result = {
            "end_point": end_point,
            "result": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
