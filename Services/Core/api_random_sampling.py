import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Common.redis_token import RedisClient
from Common.util import generate_uuid7
from Crud import genarating_synthetic_tasks, sampling
from Model.schema import (
    RandomSamplingSchema,
    ReturnMessageSchema,
    ReturnResultSchema,
)
from Services.Core.task_random_sampling import task_random_sampling_from_pkl
from Settings.Database.database import async_get_db
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/core",
)


@router.post(
    path="/request_random_sampling",  # random_sampling 은 synthetic dataset 상품의 경우에만 진행된다.
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_random_sampling(
    data: RandomSamplingSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class RandomSamplingSchema(BaseModel): \n
        synthetic_task_id:UUID = Query(...,description="synthetic_tasks_id of synthetic_task table") \n

    req_info = {
            "synthetic_project_id" : task.project_id,
            "synthetic_task_row_id":task.id,
            "synthetic_task_id":synthetic_task_id,
            "product_registration_id": task.product_registration_row.id,
            "pkl_path":task.result_files_list_pkl_path,
            "nums_of_buffer_sampling":task.product_registration_row.nums_of_buffer_sampling,
            "nums_of_current_sampling": total_table_items
            "dataset_product_quantity" : default(1000) or order테이블에 있는 dataset_product_quantity 컬럼의 값
        }
    """
    try:
        end_point = f"{router.prefix}/request_random_sampling"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        redis_client = RedisClient()
        if redis_client.is_element_in_sampling_process_list(
            str(data.synthetic_task_id)
        ):  # 랜덤샘플링이 이미 진행중인지 확인...
            logger.info(
                f"{data.synthetic_task_id}은 랜덤샘플링이 진행중입니다. ---> req_info:{data}"
            )
            result = {
                "end_point": end_point,
                "result": f"{str(data.synthetic_task_id)} 는 랜덤 샘플링 요청이 이미 task queue에 들어있는 task 입니다.",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res

        req_info = await genarating_synthetic_tasks.read(
            db=db,
            params={
                "from": "api_random_sampling()",
                "synthetic_task_id": data.synthetic_task_id,
            },
        )

        if req_info is None: # azoo_product 로 등록되어 있지 않은 상품
            result = {
            "end_point": end_point,
            "msg": f"synthetic_task_id:{str(data.synthetic_task_id)} AZOO_PRODUCT 가 등록되어 있지 않습니다. 관리자 페이지에서 상품등록을 먼저 진행 해 주세요.",
            "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=400)
            return res
        

        total_table_items = await sampling.read(
            db=db,
            params={
                "from": "api_random_sampling()",
                "product_registration_row_id": req_info["product_registration_id"],
                "synthetic_task_row_id": req_info["synthetic_task_row_id"],
            },
        )

        req_info.update({"nums_of_current_sampling": total_table_items})
        logger.info(f"req_info : {req_info}")

        try:
            # 랜덤샘플링 진행중인 task_list 목록에 task_id 등록함
            redis_client.add_sampling_process_list(str(data.synthetic_task_id))

            # celery 워커에 task push 함
            req_info["synthetic_task_id"] = str(
                req_info["synthetic_task_id"]
            )  # UUID -> str
            generated_task_id = str(generate_uuid7())
            task_id = task_random_sampling_from_pkl.apply_async(
                args=[req_info],
                queue="sampling_from_pkl_queue",
                task_id=generated_task_id,
                countdown=0.05,
            )

            logger.info(
                f"random sampling task 를 push 하였습니다.  , req_info:{req_info}"
            )
            result = {
                "end_point": end_point,
                "result": f"random sampling task 를 push 하였습니다.  , req_info:{req_info}",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res
        except:
            logger.error(
                f"Redis의 랜덤샘플링 작업 리스트 갱신에 실패하여, 랜덤샘플링 작업에 실패했습니다. ---> req_info{req_info}"
            )
            logger.error(traceback.format_exc())
            result = {
                "end_point": end_point,
                "result": f"Redis의 랜덤샘플링 작업 리스트 갱신에 실패하여, random sampling task push에 실패했습니다. ---> req_info{req_info}",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=400)
            return res

    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params(synthetic_task_id):{str(data.synthetic_task_id)} \n {traceback.format_exc()}"
        )
        logger.error(fail_message)

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res
