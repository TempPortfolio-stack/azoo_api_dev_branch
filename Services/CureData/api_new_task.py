# {
#   "project_id": 2,
#   "project_name": "string",
#   "dataset_id": "018eef11-fb5f-7486-a976-5e7705535d9c",
#   "ai_model_id": "018eea81-1421-797f-985c-7dbd03826dad",
#   "db_connection_info": {"access_key": "4C39A4937E0079E5C294", "secret_key": "82F8F03F03F505B9AE0E56926FF6E19152481937", "bucket_name": "neoda-dev", "service_name": "NAVER"}
# }

import time
import traceback
from datetime import datetime

# from fastapi_pagination import Page , Params
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Common.nas_manager import NasManager
from Crud import genarating_synthetic_tasks, product_registration
from Model.schema import NewTaskSchema, ReturnResultSchema
from Settings.Database.database import (
    async_get_db,
)
from Settings.Logger.logging_config import fastapi_logger

# from fastapi_pagination import paginate , Params
# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/cure",
)


@router.post(
    path="/new_task",
    tags=["문서 개요 3.12 : /new_task"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
    },
)
async def api_new_task(
    params: NewTaskSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    계약서를 작성한 Seller의 Data가 정상적으로 업로드되면 호출하는 API

    Seller가 정상적으로 업로드가 종료되면 dataset_id 를 부여받게 된다.
    그 부여받은 dataset_id 와 contract_id 를 매핑정보를 contract_to_dataset 테이블에 넣는것이 이 함수의 역할이다.
    정상적으로 테이블에 insert가 되면, insert된 row의 정보를 반환하자.

    class NewTaskSchema(BaseModel):    \n
        project_id:int = Query(..., description="id of Projects table")      \n
        project_name:str = Query(..., description="project_name of Projects table") \n
        dataset_id:UUID = Query(..., description="dataset_id for running task") \n
        ai_model_id:UUID = Query(..., description="ai_model_it for running task") \n
        premium_purchase_id:int|None = Query(default=None, description="[Premium] purchase_id") \n
        # dataset_path:str = Query(...,  description="product_registration 테이블의 dataset_path") \n
        # db_connection_info:dict = Query(..., description="db_connection_info for storing result dataset") \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/new_task"

        fastapi_logger.info(
            f"end_point:{end_point} 로 요청이 들어왔습니다. ---> params:{params.model_dump()}"
        )

        if params.project_id in [
            4,
            5,
        ]:  # synthetic tasks의 project id  # 4: Premium Dataset / $ 5: Premium Report

            #### 1.  여기에, NAS 에 Premium Dataset / Premium REport 디렉토리 생성
            nas_manager = NasManager(dataset_id=str(params.dataset_id))
            # dir_path = nas_manager.create_dir(type='synthetic_output')
            if (
                dir_path := nas_manager.create_dir(type="synthetic_output")
            ) is not None:
                #### 2. product_registration 테이블에서 dataset_path 컬럼에 1단계에서 만든 디렉토리 경로 update
                _ = await product_registration.update(
                    db=db,
                    params={
                        "from": "make_dir_in_NAS_for_premium_product()",
                        "product_registration_dataset_id": str(params.dataset_id),
                        "dataset_path": dir_path.split("/synthetic_output")[0],
                    },
                )
            else:
                raise Exception(
                    f"dataset_id:{params.dataset_id}가 정상적으로 생성되지 않았습니다 ---> Raise"
                )
            pass

        new_synthetic_task_row_id = await genarating_synthetic_tasks.create(
            db=db, params={"from": "api_new_task()", "req_params": params.model_dump()}
        )

        end = time.time()
        result = {
            "end_point": end_point,
            "result": f"{new_synthetic_task_row_id} record 가 정상적으로 생성되었습니다.",
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(
            f"Result:{result} // Params:{params} // row_id:{new_synthetic_task_row_id}"
        )

        ### task_id 생성하여, DB에 update하기 --> 백그라운드로 실행하면 된다.
        background_tasks.add_task(
            toss_new_task, db, str(params.dataset_id), new_synthetic_task_row_id
        )
        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{params} \n {traceback.format_exc()}"
        )
        fastapi_logger.error(fail_message)

        # error_message = str(e) + '\n' + error_traceback # 에러 메시지와 traceback 결합
        # truncated_error = error_message[:300] # 문자열을 300글자로 자름
        result = {
            "end_point": end_point,
            "result": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


async def toss_new_task(db, dataset_id: str, new_row_id: int):
    try:
        fastapi_logger.info(
            f"toss_new_task() 로 요청이 들어왔습니다 --> dataset_id:{dataset_id} , new_generating_synthetic_task_row_id:{new_row_id}"
        )

        task_id = await genarating_synthetic_tasks.update(
            db=db,
            params={
                "from": "toss_new_task()",
                "new_row_id": new_row_id,
                "dataset_id": str(dataset_id),
            },
        )

        fastapi_logger.info(
            f"generaitng_synthetic_tasks 테이블을 update하였습니다. ---> task_id:{task_id}"
        )
    except:
        fastapi_logger.error(traceback.format_exc())


#### 마이클 API 사용해서, task_id 발급받던 legacy 방식
# async def toss_new_task(params:NewTaskSchema , new_row_id:int):
#     try:
#         redis_client = RedisClient()
#         access_token = await redis_client.get_access_token()
#         if access_token is None:
#             del redis_client
#             raise Exception("액세스 토큰 발급받지 못했음 --> 예외발생")

#         fastapi_logger.info(f"access_token:{access_token}")
#         url = settings['NEW_CUREDATA_URL']

#         async with AsyncSessionLocal() as db:
#             result = await db.execute(
#                 select(ProductRegistrationModel)
#                 .where(ProductRegistrationModel.dataset_id == params.dataset_id)
#             )
#             row = result.scalars().first()
#             if row is None:
#                 raise Exception(f"dataset_id: {params.dataset_id} 가 존재하지 않습니다.")
#             dataset_path = row.dataset_path
#             product_registration_id = row.id

#         data = {
#             "service_id": str(params.ai_model_id),
#             "input_storage":{
#                 "storage_type": "nfs",
#                 "input_path": dataset_path
#             },
#             "output_storage": {
#                 "storage_type": "nfs",
#                 "output_path": f"/mnt/nfs/azoo/{settings['DEPLOY_MODE']}/{params.dataset_id}/synthetic_dataset/"
#             }
#         }

#         fastapi_logger.info(f"task data : {data} -- in background")
#         headers = {'Authorization': access_token}
#         response = requests.post(url , headers=headers , json=data)
#         fastapi_logger.info(f"Status Code : {response.status_code}")
#         if response.status_code == 200:
#             res_result = response.json()
#             task_id = res_result['data']['task_id']

#             async with AsyncSessionLocal() as db:
#                 result = await db.execute(
#                     select(GeneratingSyntheticTasksModel)
#                     .where(GeneratingSyntheticTasksModel.id == new_row_id)
#                 )
#                 new_row = result.scalars().first()

#                 if new_row:
#                     # Task ID 업데이트
#                     await db.execute(
#                         update(GeneratingSyntheticTasksModel)
#                         .where(GeneratingSyntheticTasksModel.id == new_row_id)
#                         .values(task_id=task_id , product_registration_id = product_registration_id)
#                     )
#                     await db.commit()  # 변경 사항 커밋
#                     await db.refresh(new_row)  # row 새로고침
#                 pass

#             fastapi_logger.info(f"JSON Response : {json.dumps(res_result , indent=4 , ensure_ascii=False)}")
#         else:
#             raise Exception("Failed to call Michael's curedata creation API")
#     except:
#         fastapi_logger.error(traceback.format_exc())
#     finally:
#         if 'redis_client' in locals():
#             del redis_client
