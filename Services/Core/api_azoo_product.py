import gc
import time
import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.db_util import (
    get_expired_products_list,
)
from Common.jwt import JWTBearer
from Common.redis_token import RedisClient
from Common.util import generate_uuid7
from Crud import (
    product_warehouse,
    azoo_product,
    product_registration,
    genarating_synthetic_tasks,
)
from Model.schema import (
    UploadAzooProductSchema,
    ReturnMessageSchema,
    ReturnResultSchema,
)
from Services.Core.task_prepare_azoo_product import task_prepare_azoo_product
from Services.Core.task_upload_to_s3 import task_upload_to_s3
from Settings.Database.database import async_get_db
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/azoo_product",
)


@router.post(
    path="/request_prepare_products",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_prepare_products(
    data: UploadAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    - AZOO_PRODUCT 테이블에 새 상품이 등록되면,, Robert 가 이 api를 호출함.
    - new row 를 azoo_product 에 insert 하고, status 는 Pending 으로 한 뒤,
    - 이 api를 호출할때, 해당 row 의 id를 파라미터로 같이 보냄

    class UploadAzooProductSchema(BaseModel): \n
        azoo_product_id:int = Query(...,description="row_id of azoo_product table") \n
        premium_purchase_id:int|None = Query(default=None , description="row_id of purchase table") \n
        sampling_table_id:int|None = Query(default=None , description="row_id of sampling table") \n
        synthetic_tasks_id:UUID|None = Query(default=None , description="task_id of generating_synthetic_tasks table") \n

        
    ### 여기서 random sampling 진행하면 된다.
    # req_info = {
        #                 "synthetic_task_id": str(synthetic_task_id)
        #             }
        # requests.post(url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/request_random_sampling" , json= req_info , headers=headers)
        # logger.info(f"api로 랜덤샘플링 요청을 보냈습니다. --> db_update_info: {db_update_info}")


    Original Dataset 형태로 판매되는 경우-->  , Common Synthetic Dataset 형태로 판매되는 경우 --> 랜덤샘플링 호출
    """

    try:
        end_point = f"{router.prefix}/request_prepare_products"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        product_registration_row_id , default_qty = await azoo_product.read(
            db=db,
            params={
                "from": "api_request_prepare_products()",
                "azoo_product_id": data.azoo_product_id
            },
        )

        (
            nums_of_buffer_sampling,
            nums_of_buffer_zip,
            nums_of_s3_product,
            data_type_name,
            product_type_name,
        ) = await product_registration.read(
            db=db,
            params={
                "from": "api_request_prepare_products()",
                "product_registration_row_id": product_registration_row_id,
            },
        )

        if (
            data_type_name.lower() == "Original data".lower()
        ):  # original 데이터셋 형태로 판매하는 경우.
            req_info = {
                "azoo_product_id": data.azoo_product_id,
                "azoo_product_type": "original dataset",
                "product_registration_row_id": product_registration_row_id,
                "generaitng_synthetic_task_id": None,
                "nums_of_buffer_sampling": nums_of_buffer_sampling,
                "nums_of_buffer_zip": nums_of_buffer_zip,
                "nums_of_s3_product": nums_of_s3_product,
            }

        else:  # Oridinal Dataset 외의 경우..            
            generaitng_synthetic_task_id , dataset_product_quantity = await genarating_synthetic_tasks.read(
                db=db,
                params={
                    "from": "api_request_prepare_products()",
                    "product_registration_row_id": product_registration_row_id,
                    "synthetic_tasks_id": str(data.synthetic_tasks_id),
                },
            )

            # if data.synthetic_tasks_id is not None:
            #     generaitng_synthetic_task_id = data.synthetic_tasks_id
            
            if product_type_name.lower() == "Common Dataset".lower():
                if (dataset_product_quantity == default_qty):
                    product_type_name = "Common Dataset"
                else:
                    product_type_name = "Common Dataset MultipleQty"
            

            
            if (
                product_type_name.lower() == "Common Dataset".lower()
            ):  # Common Synthetic Dataset 상품
                req_info = {
                    "azoo_product_id": data.azoo_product_id,
                    "azoo_product_type": "common synthetic dataset"   ,
                    "product_registration_row_id": product_registration_row_id,
                    "generaitng_synthetic_task_id": str(generaitng_synthetic_task_id),
                    "nums_of_buffer_sampling": nums_of_buffer_sampling,
                    "nums_of_buffer_zip": nums_of_buffer_zip,
                    "nums_of_s3_product": nums_of_s3_product,
                }
                pass
            elif (
               product_type_name.lower() == "Common Dataset MultipleQty".lower()
            ): # Common Synthetic Dataset 의 Multiple Quantity 상품 --> Premium 상품으로 취급
                req_info = {
                    "azoo_product_id": data.azoo_product_id,
                    "azoo_product_type": "premium synthetic dataset",
                    "product_registration_row_id": product_registration_row_id,
                    "generaitng_synthetic_task_id": str(generaitng_synthetic_task_id),
                    "nums_of_buffer_sampling": nums_of_buffer_sampling,
                    "nums_of_buffer_zip": nums_of_buffer_zip,
                    "nums_of_s3_product": nums_of_s3_product,
                }
                pass
            elif (
                product_type_name.lower() == "Premium Dataset".lower()
            ):  # Premium Synthetic Dataset 상품
                req_info = {
                    "azoo_product_id": data.azoo_product_id,
                    "azoo_product_type": "premium synthetic dataset",
                    "product_registration_row_id": product_registration_row_id,
                    "generaitng_synthetic_task_id": str(generaitng_synthetic_task_id),
                    "nums_of_buffer_sampling": nums_of_buffer_sampling,
                    "nums_of_buffer_zip": nums_of_buffer_zip,
                    "nums_of_s3_product": nums_of_s3_product,
                }
                pass
            elif (
                product_type_name.lower() == "Premium-Report".lower()
            ):  # Premium Report 상품
                req_info = {
                    "azoo_product_id": data.azoo_product_id,
                    "azoo_product_type": "premium report",
                    "product_registration_row_id": product_registration_row_id,
                    "generaitng_synthetic_task_id": str(generaitng_synthetic_task_id),
                    "nums_of_buffer_sampling": nums_of_buffer_sampling,
                    "nums_of_buffer_zip": nums_of_buffer_zip,
                    "nums_of_s3_product": nums_of_s3_product,
                }
                pass

        generated_task_id = str(generate_uuid7())
        task_id = task_prepare_azoo_product.apply_async(
            args=[req_info],
            queue="prepare_azoo_product_queue",
            task_id=generated_task_id,
            countdown=0.05,
        )

        result = {
            "end_point": end_point,
            "result": f"prepare_products task 를 push 하였습니다.  , req_info:{req_info}",
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


@router.post(
    path="/request_uploading",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_uploading(
    data: UploadAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    일반적인 Common Synthetic Dataset 형태의 product 를 s3로 업로딩 함 \n

    class UploadAzooProductSchema(BaseModel): \n
        azoo_product_id:int = Query(...,description="row_id of azoo_product table") \n
        premium_purchase_id:int|None = Query(default=None , description="row_id of purchase table") \n
        sampling_table_id:int|None = Query(default=None , description="row_id of sampling table") \n

    [처리순서] \n
    - packaged 된 synthetic product의 zip 파일을 s3로 올리고, nas에서 파일 삭제함 \n
    - upload 완료되면...
    - (1) sampling 테이블에서 해당 파일의 row status를 Moved_to_s3 로 바꿈
    - (2) product_warehouse 테이블에서 상태값을.. 2)Uploading  3) Failed During uploading  4) Azoo Product  로 update 한다.

    task_req_info = {
                        "azoo_product_id":azoo_product_id,
                        "sampling_id" : candidate_upload_product_row[0].sampling_row.id if candidate_upload_product_row[0].sampling_row is not None else None,
                        "mnt_path_in_nas" : candidate_upload_product_row[0].sampling_row.mnt_path_in_nas if candidate_upload_product_row[0].sampling_row is not None else None,
                        "dataset_id" : str(candidate_upload_product_row[0].sampling_row.product_registration_row.dataset_id) if candidate_upload_product_row[0].sampling_row is not None else None,
                        "product_warehouse_id" : candidate_upload_product_row[0].id if candidate_upload_product_row[0].sampling_row is not None else None,
                        "nums_of_s3_product" : candidate_upload_product_row[0].sampling_row.product_registration_row.nums_of_s3_product if candidate_upload_product_row[0].sampling_row is not None else None,
                        "cnt_of_current_azoo_product": cnt_of_current_azoo_product ,
                        "product_type_id": candidate_upload_product_row[0].sampling_row.product_registration_row.product_type_id if candidate_upload_product_row[0].sampling_row is not None else None,  # 1. Common Dataset / 2. Premium Dataset / 3. Premium-Report
                        "data_type_id": candidate_upload_product_row[0].sampling_row.product_registration_row.product_base_info_row.data_type_id if candidate_upload_product_row[0].sampling_row is not None else None,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    }
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/request_uploading"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
        redis_client = RedisClient()

        req_info_list = await product_warehouse.read(
            db=db,
            params={
                "from": "api_request_uploading()",
                "azoo_product_id": data.azoo_product_id,
            },
        )

        logger.info(
            f"len(req_info_list): {len(req_info_list)} --> req_info_list:{req_info_list}"
        )

        success_pushed_product_warehouse_id_list = []
        for req_info in req_info_list:
            try:
                # s3 업로딩 요청큐에 product_warehouse_id 를 등록함
                if redis_client.is_element_in_s3_uploading_list(
                    req_info["product_warehouse_id"]
                ):
                    logger.info(
                        f"product_warehouse_id : {req_info['product_warehouse_id']}은 s3업로드 요청 큐에 등록되어 있습니다. ---> req_info:{data}"
                    )
                else:
                    redis_client.add_s3_uploading_process_list(
                        req_info["product_warehouse_id"]
                    )
                    logger.info(
                        f"product_warehouse_id:{req_info['product_warehouse_id']} 의 s3 업로딩 요청을 redis의 s3_uploading_list 에 추가하였습니다."
                    )

                    generated_task_id = str(generate_uuid7())
                    task_id = task_upload_to_s3.apply_async(
                        args=[req_info],
                        queue="uploading_to_s3_queue",
                        task_id=generated_task_id,
                        countdown=0.05,
                    )
                    logger.info(
                        f"task_upload_to_s3 큐에 task를 push 하였습니다. ---> generated_task_id:{generated_task_id} , req_info:{req_info}"
                    )
                    success_pushed_product_warehouse_id_list.append(
                        req_info["product_warehouse_id"]
                    )
            except:
                logger.error(
                    f"s3업로딩 요청이 정상으로 Queue에 들어가지 못했습니다. ---> req_info:{req_info}"
                )
                logger.error(traceback.format_exc())
                continue
            pass

        end = time.time()
        result = {
            "end_point": end_point,
            "result": f"uploading to s3 task 를 {len(success_pushed_product_warehouse_id_list)}건 push 하였습니다.  , azoo_product_id:{str(data.azoo_product_id)} , product_warehouse_id_list:{success_pushed_product_warehouse_id_list}",
            "response_date": datetime.now(),
            "running_time": end - begin,
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


@router.post(
    path="/request_original_product_uploading",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_original_product_uploading(
    data: UploadAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    - packaged 된 original product의 zip 파일을 s3로 올리고, nas에서 파일 삭제 하지 않음 \n
    - upload 완료되면...
    - (1) product_warehouse 테이블에서 상태값을.. 2)Uploading  3) Failed During uploading  4) Azoo Product  로 update 한다.

    class UploadAzooProductSchema(BaseModel): \n
        azoo_product_id:int = Query(...,description="row_id of azoo_product table") \n
        premium_purchase_id:int|None = Query(default=None , description="row_id of purchase table") \n
        sampling_table_id:int|None = Query(default=None , description="row_id of sampling table") \n


    req_info = {
                "dataset_id" : dataset_id ,
                "product_warehouse_id" : product_warehouse_id ,
                "azoo_product_id" : azoo_product_id ,
                "mnt_path_in_nas" : mnt_path_in_nas,
                "nums_of_s3_product" : nums_of_s3_product ,
                "cnt_of_current_azoo_product": cnt_of_current_azoo_product ,
                "nums_of_buffer_zip": nums_of_buffer_zip,
                "product_type_id": candidate_upload_product_row.azoo_product.product_registration_row.product_type_id ,  # 1. Common Dataset / 2. Premium Dataset / 3. Premium-Report
                "data_type_id": candidate_upload_product_row.azoo_product.product_registration_row.product_base_info_row.data_type_id ,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
            }
    """

    try:
        end_point = f"{router.prefix}/request_original_product_uploading"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        begin = time.time()

        req_info_list = await product_warehouse.read(
            db=db,
            params={
                "from": "api_request_original_product_uploading()",
                "azoo_product_id": data.azoo_product_id,
            },
        )

        redis_client = RedisClient()

        success_pushed_product_warehouse_id_list = []
        for req_info in req_info_list:
            try:
                # s3 업로딩 요청큐에 product_warehouse_id 를 등록함
                if redis_client.is_element_in_s3_uploading_list(
                    req_info["product_warehouse_id"]
                ):
                    logger.info(
                        f"[Original Dataset]  product_warehouse_id: {req_info['product_warehouse_id']}은 s3업로드 요청 큐에 등록되어 있습니다. ---> req_info:{data}"
                    )
                else:
                    redis_client.add_s3_uploading_process_list(
                        req_info["product_warehouse_id"]
                    )
                    logger.info(
                        f"[Original Dataset] product_warehouse_id:{req_info['product_warehouse_id']} 의 s3 업로딩 요청을 redis의 s3_uploading_list 에 추가하였습니다."
                    )

                    generated_task_id = str(generate_uuid7())
                    task_id = task_upload_to_s3.apply_async(
                        args=[req_info],
                        queue="uploading_to_s3_queue",
                        task_id=generated_task_id,
                        countdown=0.05,
                    )
                    logger.info(
                        f"[Original Dataset]  task_upload_to_s3 큐에 task를 push 하였습니다. ---> generated_task_id:{generated_task_id} , req_info:{req_info}"
                    )
                    success_pushed_product_warehouse_id_list.append(
                        req_info["product_warehouse_id"]
                    )
            except:
                logger.error(
                    f"[Original Dataset]  s3업로딩 요청이 정상으로 Queue에 들어가지 못했습니다. ---> req_info:{req_info}"
                )
                logger.error(traceback.format_exc())
                continue
            pass

        end = time.time()
        result = {
            "end_point": end_point,
            "result": f"[Original Dataset] uploading to s3 task 를 {len(success_pushed_product_warehouse_id_list)}건 push 하였습니다.  , azoo_product_id:{str(data.azoo_product_id)} , product_warehouse_id_list:{success_pushed_product_warehouse_id_list}",
            "response_date": datetime.now(),
            "running_time": end - begin,
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


@router.post(
    path="/request_premium_synthetic_uploading",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_premium_synthetic_uploading(
    data: UploadAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    Premium Synthetic Dataset 형태의 product 를 s3로 업로딩 함 \n

    class UploadAzooProductSchema(BaseModel): \n
        azoo_product_id:int = Query(...,description="row_id of azoo_product table") \n
        premium_purchase_id:int = Query(...,description="row_id of purchase table") \n
        sampling_table_id:int = Query(...,description="row_id of sampling table") \n


    [처리순서] \n
    - packaged 된 synthetic product의 zip 파일을 s3로 올리고, nas에서 파일 삭제함 \n
    - upload 완료되면...
    - (1) sampling 테이블에서 해당 파일의 row status를 Moved_to_s3 로 바꿈
    - (2) product_warehouse 테이블에서 상태값을.. 2)Uploading  3) Failed During uploading  4) Azoo Product  로 update 한다.

    req_info = {
                    "azoo_product_id": azoo_product_id ,
                    "sampling_id" : sampling_table_id,
                    "current_purchase_status_id": sampling_row.product_warehouse_row.purchase_row.status_id ,
                    "mnt_path_in_nas" : sampling_row.mnt_path_in_nas, # Premium_synthetic 의 경우에는.. 랜덤샘플링 , 패키징을 처리하므로.. sampling table에 남는다.
                    "dataset_id" : sampling_row.product_registration_row.dataset_id,
                    "product_warehouse_id" : product_warehouse_row.id ,
                    "product_type_id": 2 , # 2. Premium Synthetic Dataset  ,  3. Premium-Report
                    "data_type_id": sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    "premium_purchase_id" : premium_purchase_id
                }
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/request_premium_synthetic_uploading"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
        redis_client = RedisClient()

        req_info = await product_warehouse.read(
            db=db,
            params={
                "from": "api_request_premium_synthetic_uploading()",
                "azoo_product_id": data.azoo_product_id,
                "premium_purchase_id": data.premium_purchase_id,
                "sampling_table_id": data.sampling_table_id,
            },
        )
        logger.info(f"req_info:{req_info}")

        try:
            # s3 업로딩 요청큐에 product_warehouse_id 를 등록함
            if redis_client.is_element_in_s3_uploading_list(
                req_info["product_warehouse_id"]
            ):
                logger.info(
                    f"product_warehouse_id : {req_info['product_warehouse_id']}은 s3업로드 요청 큐에 등록되어 있습니다. ---> req_info:{data}"
                )
            else:
                redis_client.add_s3_uploading_process_list(
                    req_info["product_warehouse_id"]
                )
                logger.info(
                    f"product_warehouse_id:{req_info['product_warehouse_id']} 의 s3 업로딩 요청을 redis의 s3_uploading_list 에 추가하였습니다."
                )

                generated_task_id = str(generate_uuid7())
                task_id = task_upload_to_s3.apply_async(
                    args=[req_info],
                    queue="uploading_to_s3_queue",
                    task_id=generated_task_id,
                    countdown=0.05,
                )
                logger.info(
                    f"task_upload_to_s3 큐에 task를 push 하였습니다. ---> generated_task_id:{generated_task_id} , req_info:{req_info}"
                )
        except:
            logger.error(
                f"s3업로딩 요청이 정상으로 Queue에 들어가지 못했습니다. ---> req_info:{req_info}"
            )
            logger.error(traceback.format_exc())

        end = time.time()
        result = {
            "end_point": end_point,
            "result": f"uploading to s3 task :{task_id} 를 Queue에  push 하였습니다.  , azoo_product_id:{str(data.azoo_product_id)} ,  sampling_table_id:{data.sampling_table_id}",
            "response_date": datetime.now(),
            "running_time": end - begin,
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


@router.post(
    path="/request_premium_report_uploading",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_request_premium_report_uploading(
    data: UploadAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    Premium Report 형태의 product 를 s3로 업로딩 함 \n

    class UploadAzooProductSchema(BaseModel): \n
        azoo_product_id:int = Query(...,description="row_id of azoo_product table") \n
        premium_purchase_id:int = Query(...,description="row_id of purchase table") \n
        sampling_table_id:int = Query(...,description="row_id of sampling table") \n


    [처리순서] \n
    - packaged 된 premium report의 zip 파일을 s3로 올리고, nas에서 파일 삭제함 \n
    - upload 완료되면...
    - (1) sampling 테이블에서 해당 파일의 row status를 Moved_to_s3 로 바꿈
    - (2) product_warehouse 테이블에서 상태값을.. 2)Uploading  3) Failed During uploading  4) Azoo Product  로 update 한다.

    req_info = {
                    "azoo_product_id": azoo_product_id ,
                    "sampling_id" : sampling_table_id,
                    "current_purchase_status_id": sampling_row.product_warehouse_row.purchase_row.status_id ,
                    "mnt_path_in_nas" : sampling_row.mnt_path_in_nas, # Premium_synthetic 의 경우에는.. 랜덤샘플링 , 패키징을 처리하므로.. sampling table에 남는다.
                    "dataset_id" : sampling_row.product_registration_row.dataset_id,
                    "product_warehouse_id" : product_warehouse_row.id ,
                    "product_type_id": 3 , # 3. Premium Synthetic Dataset  ,  3. Premium-Report
                    "data_type_id": sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    "premium_purchase_id" : premium_purchase_id
                }
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/request_premium_report_uploading"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
        redis_client = RedisClient()

        req_info = await product_warehouse.read(
            db=db,
            params={
                "from": "api_request_premium_report_uploading()",
                "azoo_product_id": data.azoo_product_id,
                "premium_purchase_id": data.premium_purchase_id,
                "sampling_table_id": data.sampling_table_id,
            },
        )
        logger.info(f"req_info:{req_info}")

        try:
            # s3 업로딩 요청큐에 product_warehouse_id 를 등록함
            if redis_client.is_element_in_s3_uploading_list(
                req_info["product_warehouse_id"]
            ):
                logger.info(
                    f"product_warehouse_id : {req_info['product_warehouse_id']}은 s3업로드 요청 큐에 등록되어 있습니다. ---> req_info:{data}"
                )
            else:
                redis_client.add_s3_uploading_process_list(
                    req_info["product_warehouse_id"]
                )
                logger.info(
                    f"product_warehouse_id:{req_info['product_warehouse_id']} 의 s3 업로딩 요청을 redis의 s3_uploading_list 에 추가하였습니다."
                )

                generated_task_id = str(generate_uuid7())
                task_id = task_upload_to_s3.apply_async(
                    args=[req_info],
                    queue="uploading_to_s3_queue",
                    task_id=generated_task_id,
                    countdown=0.05,
                )
                logger.info(
                    f"task_upload_to_s3 큐에 task를 push 하였습니다. ---> generated_task_id:{generated_task_id} , req_info:{req_info}"
                )
        except:
            logger.error(
                f"s3업로딩 요청이 정상으로 Queue에 들어가지 못했습니다. ---> req_info:{req_info}"
            )
            logger.error(traceback.format_exc())

        end = time.time()
        result = {
            "end_point": end_point,
            "result": f"uploading to s3 task :{task_id} 를 Queue에  push 하였습니다.  , azoo_product_id:{str(data.azoo_product_id)} ,  sampling_table_id:{data.sampling_table_id}",
            "response_date": datetime.now(),
            "running_time": end - begin,
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


@router.get(
    path="/expired_products_list",
    tags=["AZOO PRODUCT"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_expired_products_list(
    db=Depends(async_get_db), access_token=Depends(JWTBearer())
):  # access_token = Depends(JWTBearer())
    """
    다운로드 기간이 지난 product_warehouse 테이블의 rows_id 리스트를 반환함
    """

    try:
        end_point = f"{router.prefix}/expired_products_list"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. ")
        row_ids = await get_expired_products_list(db)

        logger.info(
            f"현재시점 기준: {datetime.now()} // expired_date 가 지난, product_warehouse 의 ids 는 다음과 같습니다--> {row_ids}"
        )
        pass

        result = {
            "end_point": end_point,
            "result": f"{row_ids}",
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
    finally:
        gc.collect()
