import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Common.redis_token import RedisClient
from Common.util import generate_uuid7
from Crud import sampling
from Model.schema import (
    ReturnMessageSchema,
    ReturnResultSchema,
    PackagingSchema,
)
from Services.Core.task_packaging import task_packaging_of_product
from Settings.Database.database import async_get_db
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/core",
)


@router.post(
    path="/request_packaging",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_packaging(
    data: PackagingSchema, db=Depends(async_get_db), access_token=Depends(JWTBearer())
):  # access_token = Depends(JWTBearer())
    """
    # Synthetic / Premium Synthetic / Premium Report


    Premium Synthetic Dataset 상품의 경우는, synthetic_task_row_id 에서, premium_purchase_id 값을 가져와야한다.


    class PackagingSchema(BaseModel): \n
        product_registration_id:int = Query(...,description="row_id of product_registration table") \n
        synthetic_task_id:UUID|None = Query( default=None ,description="task_id of generating_synthetic_tasks table") # 이거 있으면, Premium-Report  \n
        synthetic_task_row_id:int|None = Query( default=None ,description="row_id of generating_synthetic_tasks table") # 이거 있으면, Premium Synthetic Dataset / None 이면 Common Synthetic Dataset \n
        is_premium_report:bool|None = Query( default=None ,description="is premium report") \n
        new_sampling_row_id:int|None = Query( default=None ,description="Premium Synthetic Dataset 의 경우.. sampling 테이블에 insert된 row_id 정보가 필요하다.") \n
    """

    try:
        end_point = f"{router.prefix}/request_packaging"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
        redis_client = RedisClient()

        if data.is_premium_report == True:  # Premium Report
            logger.info(f"data.is_premium_report == True 분기에서 처리합니다. --> {data}")
            req_info_list = await sampling.read(
                db=db,
                params={
                    "from": "api_request_packaging_for_Premium_Report()",
                    "product_registration_id": data.product_registration_id,
                    "synthetic_task_id": str(data.synthetic_task_id),
                },
            )

            """            
            req_info = {
                "azoo_product_id" : premium_report_azoo_product_id,
                "premium_purchase_id": purchase_row.id,
                "product_type_id": sampling_row.product_registration_row.product_type_id, # 1:Common Dataset / 2:Premium Dataset / 3:Premium-Report
                "sampling_table_row_id": sampling_row.id,
                "product_registration_id":product_registration_id,
                "random_sampled_list" : sampling_row.random_sampled_list,
                "pkl_path": sampling_row.synthetic_task_row.result_files_list_pkl_path,
                "original_dataset_path" : sampling_row.product_registration_row.dataset_path,
                "target_cnt_of_zipfiles" : 1, # premium_report 경우는 매번 새롭게 1개의 상품만 만들것이므로 target은 1로 한다.
                "current_cnt_of_zip_files" : 0,  # premium_report 경우는 매번 새롭게 1개의 상품만 만들것이므로
                "synthetic_task_id": sampling_row.synthetic_task_row.task_id , 
                "result_dataset_path" : sampling_row.synthetic_task_row.result_dataset_path ### 프리미엄 리포트의 경우만 있는것이다.
            }
            """

        elif (
            data.synthetic_task_row_id is not None
        ):  # 이 값이 있다는 것은, Premium Synthetic Dataset 이라는 의미.
            logger.info(f"data.synthetic_task_row_id is not None 분기에서 처리합니다. --> {data}")
            req_info_list = await sampling.read(
                db=db,
                params={
                    "from": "api_request_packaging_for_Premium_Synthetic_Dataset()",
                    "product_registration_id": data.product_registration_id,
                    "synthetic_task_row_id": data.synthetic_task_row_id,
                    "new_sampling_row_id": data.new_sampling_row_id,
                },
            )
            """
            req_info = {
                "azoo_product_id" : premium_dataset_azoo_product_id,
                "premium_purchase_id": purchase_row.id,
                "product_type_id":sampling_row.product_registration_row.product_type_id, # 1:Common Dataset / 2:Premium Dataset / 3:Premium-Report
                "sampling_table_row_id": sampling_row.id,
                "product_registration_id":product_registration_id,
                "random_sampled_list" : sampling_row.random_sampled_list,
                "pkl_path": sampling_row.synthetic_task_row.result_files_list_pkl_path,
                "original_dataset_path":sampling_row.product_registration_row.dataset_path,
                "target_cnt_of_zipfiles" : 1, # premium_synthetic_dataset의 경우는 매번 새롭게 1개의 상품만 만들것이므로 target은 1로 한다.
                "current_cnt_of_zip_files" : 0 # premium_synthetic_dataset의 경우는 매번 새롭게 1개의 상품만 만들것이므로 current zip 파일은 0개로 한다.
            }
            """

        elif (data.is_premium_report is None) and (
            data.synthetic_task_row_id is None
        ):  # is_premium , synthetic_task_row_id 가 둘 다 None 이면 ---> Common Synthetic Dataset 이라는 뜻
            logger.info(f"(data.is_premium_report is None) and (data.synthetic_task_row_id is None) 분기에서 처리합니다. --> {data}")

            req_info_list = await sampling.read(
                db=db,
                params={
                    "from": "api_request_packaging()",
                    "product_registration_id": data.product_registration_id,
                },
            )
            """
            req_info = {
                "azoo_product_id":candidate_sampling_row[0].product_registration_row.azoo_product[0].id,
                "product_type_id":candidate_sampling_row[0].product_registration_row.product_type_id, # 1:Common Synthetic Dataset / 2:Premium Synthetic Dataset / 3:Premium-Report
                "sampling_table_row_id": candidate_sampling_row[0].id,
                "product_registration_id":product_registration_id,
                "random_sampled_list" : candidate_sampling_row[0].random_sampled_list,
                "pkl_path": candidate_sampling_row[0].synthetic_task_row.result_files_list_pkl_path,
                "original_dataset_path":candidate_sampling_row[0].product_registration_row.dataset_path,
                "target_cnt_of_zipfiles" : candidate_sampling_row[0].product_registration_row.nums_of_buffer_zip,
                "current_cnt_of_zip_files" : cnt_of_current_packaged
            }
            """

        else:
            logger.info(f"유효한 요청이 아닙니다. 요청 data를 확인하세요. --->   data:{data}")
            raise Exception(
                f"유효한 요청이 아닙니다. 요청 data를 확인하세요. --->   data:{data}"
            )

        success_pushed_sampling_id_list = []
        for req_info in req_info_list:
            try:
                if not redis_client.is_element_in_package_list(
                    req_info["sampling_table_row_id"]
                ):  # 패키징 요청이 이미 등록된 sampling 의 row_id 인지 확인
                    # 패키징 요청 task_list 목록에 sampling_row_id 등록함
                    redis_client.add_package_process_list(
                        req_info["sampling_table_row_id"]
                    )
                    logger.info(
                        f"sampling_table_row_id:{req_info['sampling_table_row_id']} 의 패키징 요청을 redis의 package_list 에 추가하였습니다."
                    )

                generated_task_id = str(generate_uuid7())
                task_id = task_packaging_of_product.apply_async(
                    args=[req_info],
                    queue="packaging_queue",
                    task_id=generated_task_id,
                    countdown=0.05,
                )
                logger.info(
                    f"task_packaging_of_product 큐에 task를 push 하였습니다. ---> generated_task_id:{generated_task_id} , req_info:{req_info}"
                )
                success_pushed_sampling_id_list.append(
                    req_info["sampling_table_row_id"]
                )
            except:
                logger.error(
                    f"패키징 요청이 정상으로 Queue에 들어가지 못했습니다. ---> req_info:{req_info}"
                )
                logger.error(traceback.format_exc())
                continue
            pass

        result = {
            "end_point": end_point,
            "result": f"packaging task 를 {len(success_pushed_sampling_id_list)}건 push 하였습니다. sampling_id_list:{success_pushed_sampling_id_list}",
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
