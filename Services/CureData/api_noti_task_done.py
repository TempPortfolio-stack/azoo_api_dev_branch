# {
#   "task_id": "018f08a2-c7ee-7978-b238-a87932523236",
#   "begin_at": "2024-04-23 01:47:16",
#   "end_at": "2024-04-23 03:47:16",
#   "task_status": "SUCCESS",
#   "output": {
#     "dataset_score": "777",
#     "dataset_report_html_file_url": "www.daum.net"
#   }
# }

import gc
import json
import os
import time
import traceback
from datetime import datetime

import requests

# from fastapi_pagination import Page , Params
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# from fastapi_pagination import paginate , Params
# from sqlalchemy.sql.expression import func
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from Common.jwt import JWTBearer
from Common.redis_token import RedisClient
from Common.db_util import update_purchase_table_status
from Common.util import generate_uuid7
from Common.util import get_azoo_fastapi_access_token
from Crud import purchase, genarating_synthetic_tasks
from Model.new_models import (
    GeneratingSyntheticTasksModel,
    MgeneratingSyntheticTaskStatus,
    ProductRegistrationModel,
    ProductBaseInfoModel,
)
from Model.schema import NotiTaskDoneSchema, ReturnResultSchema
from Services.Core.task_syn_output_files_to_pkl import task_syn_output_files_to_pkl
from Settings.Database.database import async_get_db
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger

router = APIRouter(
    prefix="/cure",
)


@router.patch(
    path="/noti_task_done",
    tags=["문서 개요 3.14 /noti_task_done"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": ReturnResultSchema,
        },  # ReturnListDataSchema[SellerNotiUploadedSchema]
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
    },
)
async def api_noti_task_done(
    params: NotiTaskDoneSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):
    """
    AI 엔진에서 curedata task 작업이 끝나고 호출되는 API

    Seller가 정상적으로 업로드가 종료되면 dataset_id 를 부여받게 된다.
    그 부여받은 dataset_id 와 contract_id 를 매핑정보를 contract_to_dataset 테이블에 넣는것이 이 함수의 역할이다.
    정상적으로 테이블에 insert가 되면, insert된 row의 정보를 반환하자.
    class NotiTaskDoneSchema(BaseModel): \n
        task_id:UUID = Query(... , description="task_id 값이다, 하지만 마이클이 ai모델의 결과zip 파일의 dataset_id 로도 사용한다. 이 값을 이용해서 download한다.") \n
        begin_at:datetime = Query(... , description="task 시작시간") \n
        end_at:datetime = Query(... , description="task 종료시간") \n
        task_status:str = Query(... , description="PENDING , RUNNING, CANCELED , SUCCESS , FAILED") \n
        output:TaskDoneOutputSchema = Query(... , description="result dataset 의 accuracy 점수와 report json 파일들이 들어있는 parent 디렉토리") \n
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/noti_task_done"

        task_id = params.task_id
        begin_at = params.begin_at
        end_at = params.end_at
        task_result_status = params.task_status
        # report_html_file_url = params.output.dataset_report_html_file_url
        dataset_score = params.output.dataset_score
        output_path = params.output.output_path

        try:
            redis_client = RedisClient()
            if redis_client.is_element_in_syn_pkl_list(str(task_id)):
                fastapi_logger.info(
                    f"synthetic_task_id : {str(task_id)} 는 이미 syn_pkl 작업 요청이 등록되어있습니다."
                )
                result = {
                    "end_point": end_point,
                    "result": f"synthetic_task_id : {str(task_id)} 는 이미 syn_pkl 작업 요청이 등록되어있습니다.",
                    "response_date": datetime.now(),
                }
                res = JSONResponse(content=jsonable_encoder(result), status_code=200)
                return res

            query = (
                select(GeneratingSyntheticTasksModel)
                .options(
                    selectinload(GeneratingSyntheticTasksModel.product_registration_row)
                    .selectinload(ProductRegistrationModel.product_base_info_row)
                    .selectinload(ProductBaseInfoModel.data_format),
                    selectinload(
                        GeneratingSyntheticTasksModel.product_registration_row
                    ).selectinload(ProductRegistrationModel.azoo_product),
                )
                .filter(GeneratingSyntheticTasksModel.task_id == task_id)
            )

            result = await db.execute(query)
            row = result.scalars().first()

            if row is None:
                fastapi_logger.error(
                    f"task_id:{task_id}에 해당되는 row 발견되지 않음!!"
                )
                raise ValueError(
                    f"GeneratingSyntheticTasksModel 테이블에서 task_id == {task_id} 인 row 가 발견되지 않음"
                )

            if row.task_status_id >= 6:  # 6. Inserting_to_DB , 7. Inserted_to_DB
                fastapi_logger.error(
                    f"task_id:{task_id}에 해당되는 row는 이미 /noti_task_done 요청을 받아 처리된(중인) row 입니다."
                )
                raise Exception(
                    f"해당되는 row는 이미 /noti_task_done 요청을 받아 처리된(중인) row 입니다."
                )

            subquery_status_id = (
                select(MgeneratingSyntheticTaskStatus.id)
                .filter(
                    func.lower(MgeneratingSyntheticTaskStatus.name)
                    == task_result_status.lower()
                )
                .subquery()
            )
            row.task_status_id = subquery_status_id
            row.created_at = begin_at
            row.updated_at = datetime.now()  # func.now()
            row.completed_at = end_at
            # row.dataset_report_html_file_url = report_html_file_url
            row.dataset_score = dataset_score
            row.result_dataset_path = output_path
            await db.commit()
            await db.refresh(row)
        except SQLAlchemyError as e:
            raise
        except Exception as e:
            await db.rollback()
            fastapi_logger.error(f"DB 작업 에러발생 --> 롤백 :: {str(e)}")
            raise

        end = time.time()

        ###### 여기서, 백그라운드 worker 동작 시킨다.
        req_info = {
            "synthetic_table_id": row.id,
            "synthetic_task_id": str(row.task_id),
            "result_dataset_path": row.result_dataset_path,
            "dataset_format_type": row.product_registration_row.product_base_info_row.data_format.name,
            "project_id": row.project_id,  # 4. Premium Dataset / 5. Premium Report
            "azoo_product_id": (
                None
                if (row.product_registration_row.azoo_product is None) or (len(row.product_registration_row.azoo_product) ==0)
                else row.product_registration_row.azoo_product[0].id
            ),
        }
        if params.task_status.lower() == "success" and row.project_id in [
            1,
            2,
            3,
            4,
        ]:  # [1,2,3]: Common Synthetic Dataset / 4 : Premium Synthetic Dataset
            ### 기존 flow 를 사용한다. :: syn_pkl_file 워커 작업이 종료되면, 자동으로 random_sampling 작업을 요청한다.
            message = f"task_id:{task_id} 는 success 되었으므로, syn_pkl_file 작업을 진행합니다...[ Common/Premium Synthetic Dataset]"
            background_tasks.add_task(toss_syn_files_to_pkl, req_info)
            fastapi_logger.info(
                f"백그라운드 워커로 toss_syn_files_to_pkl() 실행합니다.---> req_info:{req_info}"
            )

        elif (
            params.task_status.lower() == "success" and row.project_id == 5
        ):  # 5. Premium Report

            if not os.path.exists(f"{output_path}/evaluate.json"):
                raise HTTPException(
                    status_code=404,
                    detail=f"{output_path}/evaluate.json 이 존재하지 않습니다. --> synthetic_table_id:{row.id}에 대한 작업을 진행할 수 없습니다.",
                )

            report_json_path = f"{output_path}/evaluate.json"
            background_tasks.add_task(update_report_json_to_db, report_json_path , row.id)
            fastapi_logger.info(
                f"백그라운드 워커로 update_report_json_to_db() 실행합니다.---> report_json_path:{report_json_path}"
            )

        result = {
            "end_point": end_point,
            "result": f"GeneratingSyntheticTasksModel id:{row.id}의 row 값이 정상적으로 Update 되었습니다.",
            "working_time_sec": end - begin,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(f"Result:{result} // Params:{params}")
        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{params} \n {traceback.format_exc()}"
        )
        fastapi_logger.error(fail_message)

        result = {
            "end_point": end_point,
            "result": "Error",
            "response_date": datetime.now(),
        }

        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res



async def update_report_json_to_db(report_json_path:str , synthetic_task_row_id:int):
    try:
        fastapi_logger.info(f"update_report_json_to_db() 에서, Report Json 파일을 DB에 업데이트 합니다. --> synthetic_task_row_id:{synthetic_task_row_id} , report_json_path:{report_json_path}")
        db = await async_get_db().__anext__()
        ##### evaluate.json 파일 내용, 테이블에 업데이트 해야 함
        with open(report_json_path, "r") as file:
            report_dict = json.load(file)
            await genarating_synthetic_tasks.update(
                db=db,
                params={
                    "from": "api_noti_task_done()_premium_report",
                    "synthetic_table_id": synthetic_task_row_id,
                    "json_report": report_dict,
                },
            )
        #### 성공적으로 Update 되었으면 --> purchase 테이블에서, 상태코드를 변경해 줘야 한다.        
        
        current_purchase_status_id , target_purchase_row_id = await purchase.read(db=db , params={"from":"update_report_json_to_db()",
                                                                                                  "synthetic_task_row_id":synthetic_task_row_id
                                                                                                })
        
        status_to = None
        if current_purchase_status_id == 3: # Premium Pending
            # --> Premium Report Active 로 바꿔야 함
            status_to = "Premium Report Active"
            pass
        elif current_purchase_status_id == 4: # Premium Dataset Active
            # --> Active 로 바꿔야 함
            status_to = "Active"
            pass
        # elif current_purchase_status_id in [1,2,5]: # 1.Active     2.Refunded     3.Premium Report Active
        #     # --> 아무것도 안 함
        #     pass

        fastapi_logger.info(f"current_purchase_status_id:{current_purchase_status_id} , target_purchase_row_id:{target_purchase_row_id} , status_to:{status_to}")

        if status_to is not None:
            await update_purchase_table_status( db = db, purchase_row_id = target_purchase_row_id, status_to=status_to)
        pass
    except:        
        fastapi_logger.error(traceback.format_exc())
    pass




async def toss_syn_files_to_pkl(
    req_info: dict,
):  # project_id 가 1,2,3 에 속하는 synthetic_tasks 결과파일들을 처리하기 위한 pkl 파일을 생성한다.
    try:
        redis_client = RedisClient()
        synthetic_task_id = req_info["synthetic_task_id"]  # UUID
        redis_client.add_syn_pkl_process_list(
            synthetic_task_id
        )  # Redis의 syn_pkl_list 리스트에 해당 synthetic_tasks 테이블의 synthetic_task_id 를 추가함

        generated_task_id = str(generate_uuid7())
        task_id = task_syn_output_files_to_pkl.apply_async(
            args=[req_info],
            queue="syn_files_to_pkl_queue",
            task_id=generated_task_id,
            countdown=0.05,
        )
    except:
        fastapi_logger.error(
            f"/noti_task_done 에서 요청된, toss_syn_files_to_pkl() 작업에 실패하였습니다. --> req_info:{req_info}"
        )
        fastapi_logger.error(traceback.format_exc())
    finally:
        if "redis_client" in locals():
            del redis_client
        gc.collect()
