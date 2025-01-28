import traceback
from datetime import datetime

from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.db_util import (
    get_user_email_addr,
)
from Common.jwt import JWTBearer
from Common.util import (
    send_email_of_ready_premium_product,
)
from Model.schema import (
    NotiPurchasedSchema,
    ReturnMessageSchema,
    ReturnResultSchema,
)
from Settings.Database.database import async_get_db
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/email",
)


@router.post(
    path="/send_of_premium_done",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_send_of_premium_done(
    data: NotiPurchasedSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class NotiPurchasedSchema(BaseModel):    \n
        purchase_id:int = Query(...,description="row id of purchase table")
    """
    try:
        end_point = f"{router.prefix}/send_of_premium_done"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        user_email = await get_user_email_addr(db=db, purchase_row_id=data.purchase_id)
        background_tasks.add_task(
            send_email_of_ready_premium_product, target_email_addr=user_email
        )
        logger.info(
            f"purchase_id: {data.purchase_id} 에 해당되는 --->  user_email_addr:{user_email} 로 이메일을 전송합니다."
        )

        result = {
            "end_point": end_point,
            "result": f"purchase_id: {data.purchase_id} 에 해당되는 --->  user_email_addr:{user_email} 로 이메일을 전송합니다.",
            "response_date": datetime.now(),
        }

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:None \n {traceback.format_exc()}"
        )
        logger.error(fail_message)

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
