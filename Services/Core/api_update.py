import traceback
from datetime import datetime

import requests
from fastapi import APIRouter, Depends, BackgroundTasks
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.db_util import (
    update_product_table_status,
    update_contract_table_status,
    update_synthetic_task_table_status,
    insert_sampled_list_to_table,
    update_azoo_product_table_status,
    get_dataset_id_by_purchase_id,
    update_sampling_table_status,
    insert_product_warehouse_table,
    update_product_warehouse_table_status,
    get_product_warehouse_row_id,
    insert_product_warehouse_table_of_original_dataset,
    update_purchase_table_status,
)
from Common.jwt import JWTBearer
from Common.util import (
    call_azoo_api,
)
from Common.util import get_azoo_fastapi_access_token
from Crud import product_warehouse
from Model.schema import (
    UploadateUploadStatusSchema,
    UploadateContractStatusSchema,
    UploadateSamplingStatusSchema,
    UpdateWareHouseSchema,
    UpdateAzooProductSchema,
    NotiPurchasedSchema,
    NotiCanceledSchema,
    ReturnMessageSchema,
    ReturnResultSchema,
    UploadateSynTaskStatusSchema,
    InsertSamplingListSchema,
    InsertWareHouseSchema,
    UpdatePurchaseSchema,
)
from Settings.Database.database import async_get_db
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger as logger

# from sqlalchemy.sql.expression import func


router = APIRouter(
    prefix="/core",
)


@router.post(
    path="/update_upload_status",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_upload_status(
    data: UploadateUploadStatusSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    dataset_id:UUID \n
    type:str = "sample" / "report_html" / "original_dataset"  \n

    dataset_id:str = Query(...,description="UUID")  \n
    status_to:str = Query(...,description="wanna status to this")    \n
    """

    try:
        end_point = f"{router.prefix}/update_upload_status"

        await update_product_table_status(
            db=db, target_dataset_id=data.dataset_id, status_to=data.status_to
        )
        logger.info(f"data:{data} 로 product_base_info 테이블을 update 합니다.")

        result = {
            "end_point": end_point,
            "result": f"data:{data} 로 product_base_info 테이블을 update 합니다.",
            "response_date": datetime.now(),
        }
        pass

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
    path="/update_contract_status",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_contract_status(
    data: UploadateContractStatusSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UploadateContractStatusSchema(BaseModel): \n
        target_table:str = Query(...,description="table name") \n
        contract_id:str = Query(...,description="contract unique id") \n
        status_to:str = Query(...,description="wanna status to this")   \n
    """

    try:
        end_point = f"{router.prefix}/update_contract_status"

        if data.target_table == "contract":
            await update_contract_table_status(
                db=db, contract_unique_id=data.contract_id, status_to=data.status_to
            )
            logger.info(f"data:{data} 로 contract 테이블을 update 합니다.")

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 contract 테이블을 update 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
    path="/update_synthetic_task_status",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_synthetic_task_status(
    data: UploadateSynTaskStatusSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UploadateSynTaskStatusSchema(BaseModel): \n
        target_table:str = Query(...,description="table name") \n
        row_id:str = Query(...,description="row id of ") \n
        result_files_list_pkl_path = Query(...,description="output pkl file path") \n
        status_to:str = Query(...,description="wanna status to this") \n
        cnt_rows_of_pkl:int = Query(...,description="length of the dataframe rows") \n
    """

    try:
        end_point = f"{router.prefix}/update_synthetic_task_status"

        if data.target_table == "genarating_synthetic_tasks":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
            await update_synthetic_task_table_status(
                db=db,
                row_id=data.row_id,
                status_to=data.status_to,
                result_files_list_pkl_path=data.result_files_list_pkl_path,
                cnt_rows_of_pkl=data.cnt_rows_of_pkl,
            )
            logger.info(
                f"data:{data} 로 genarating_synthetic_tasks 테이블을 update 합니다."
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 genarating_synthetic_tasks 테이블을 update 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
    path="/insert_sampled_list",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_insert_sampled_list(
    data: InsertSamplingListSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class InsertSamplingListSchema(BaseModel): \n
        target_table:str = Query(...,description="table name") \n
        product_registration_id:int = Query(...,description="row id of product_registration table") \n
        generating_synthetic_task_id:int = Query(... , description="row id of synthetic task table") \n
        sampled_list:list = Query(... , description="random sampled list") \n
    """

    try:
        end_point = f"{router.prefix}/insert_sampled_list"

        if data.target_table == "sampling":
            logger.info(
                f"{end_point} 로 요청이 들어왔습니다. --> target_table:{data.target_table} , product_registration_id:{data.product_registration_id} , generating_synthetic_task_id:{data.generating_synthetic_task_id} , len(sampled_list):{len(data.sampled_list)}"
            )

            new_sampling_row_id = await insert_sampled_list_to_table(
                db=db,
                sampled_list=data.sampled_list,
                product_registration_row_id=data.product_registration_id,
                synthetic_task_id=data.generating_synthetic_task_id,
            )

            result = {
                "end_point": end_point,
                "result": f"new_sampling_row_id:{new_sampling_row_id} 가 sampling 테이블에 insert 되었습니다.",
                "new_sampling_row_id": new_sampling_row_id,
                "response_date": datetime.now(),
            }
            pass

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
    path="/update_sampling_status",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_sampling_status(
    data: UploadateSamplingStatusSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UploadateSamplingStatusSchema(BaseModel):  \n
        target_table:str = Query(...,description="table name") \n
        row_id:int = Query(...,description="sampling table row id") \n
        status_to:str = Query(...,description="wanna status to this")  \n
        mnt_path_in_nas:str = Query(...,description="zipfile path in the nas")  \n
    """

    try:
        end_point = f"{router.prefix}/update_sampling_status"

        if data.target_table == "sampling":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")
            await update_sampling_table_status(
                db=db,
                row_id=data.row_id,
                status_to=data.status_to,
                mnt_path_in_nas=data.mnt_path_in_nas,
            )
            logger.info(
                f"data:{data} 로 genarating_synthetic_tasks 테이블을 update 합니다."
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 genarating_synthetic_tasks 테이블을 update 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
    path="/insert_product_warehouse",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_insert_product_warehouse(
    data: InsertWareHouseSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class InsertWareHouseSchema(BaseModel):  \n
        target_table:str = Query(...,description="table name")  \n
        azoo_product_id:int = Query(...,description="row id of azoo_product table")  \n
    """

    try:
        end_point = f"{router.prefix}/insert_product_warehouse"

        if data.target_table == "product_warehouse":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            await insert_product_warehouse_table(
                db=db, azoo_product_id=data.azoo_product_id, status="Pending"
            )  # Common Synthetic Dataset Product
            logger.info(
                f"data:{data} 로 [Common Synthetic] product_warehouse 테이블을 insert 합니다."
            )

            # /request_uploading 을 호출한다.
            background_tasks.add_task(
                toss_req_uploading,
                {
                    "product_type": "common synthetic dataset",
                    "to_": "request_uploading",
                    "azoo_product_id": data.azoo_product_id,
                },
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 product_warehouse 테이블을 insert 합니다.",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res
        else:
            raise Exception("옳지 않은 요청입니다.")
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
    path="/insert_premium_synthetic_product_warehouse",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_insert_premium_synthetic_product_warehouse(
    data: InsertWareHouseSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class InsertWareHouseSchema(BaseModel):  \n
        target_table:str = Query(...,description="table name")  \n
        azoo_product_id:int = Query(...,description="row id of azoo_product table")  \n
        premium_purchase_id:int|None = Query(default=None , description="premium_purchase_id") \n
        sampling_table_row_id:int|None = Query(default=None , description="sampling_table_row_id") \n
    """

    try:
        end_point = f"{router.prefix}/insert_premium_synthetic_product_warehouse"

        if data.target_table == "product_warehouse":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            product_ware_house_row_id = await product_warehouse.create(
                db=db,
                params={
                    "from": "api_insert_premium_synthetic_product_warehouse()",
                    "azoo_product_id": data.azoo_product_id,
                    "premium_purchase_id": data.premium_purchase_id,
                    "sampling_table_row_id": data.sampling_table_row_id,
                    "set_status_to": "Pending",
                },
            )

            logger.info(
                f"data:{data} 로 [Premium Synthetic] product_warehouse 테이블에 insert 하였습니다. ---> product_warehouse_row_id : {product_ware_house_row_id}"
            )

            # background_tasks.add_task(toss_req_uploading , data.azoo_product_id , 'request_premium_synthetic_uploading') # /request_premium_synthetic_uploading 을 호출한다.

            # /request_uploading 을 호출한다.
            background_tasks.add_task(
                toss_req_uploading,
                {
                    "product_type": "premium synthetic dataset",
                    "to_": "request_premium_synthetic_uploading",
                    "azoo_product_id": data.azoo_product_id,
                    "premium_purchase_id": data.premium_purchase_id,
                    "sampling_table_id": data.sampling_table_row_id,
                },
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 product_warehouse 테이블을 insert 합니다.",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res
        else:
            raise Exception("옳지 않은 요청입니다.")
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{data} \n {traceback.format_exc()}"
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
    path="/insert_premium_report_product_warehouse",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_insert_premium_report_product_warehouse(
    data: InsertWareHouseSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class InsertWareHouseSchema(BaseModel):  \n
        target_table:str = Query(...,description="table name")  \n
        azoo_product_id:int = Query(...,description="row id of azoo_product table")  \n
        premium_purchase_id:int|None = Query(default=None , description="premium_purchase_id") \n
        sampling_table_row_id:int|None = Query(default=None , description="sampling_table_row_id") \n
    """

    try:
        end_point = f"{router.prefix}/insert_premium_report_product_warehouse"

        if data.target_table == "product_warehouse":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            product_ware_house_row_id = await product_warehouse.create(
                db=db,
                params={
                    "from": "api_insert_premium_report_product_warehouse()",
                    "azoo_product_id": data.azoo_product_id,
                    "premium_purchase_id": data.premium_purchase_id,
                    "sampling_table_row_id": data.sampling_table_row_id,
                    "set_status_to": "Pending",
                },
            )

            logger.info(
                f"data:{data} 로 [Premium Synthetic] product_warehouse 테이블에 insert 하였습니다. ---> product_warehouse_row_id : {product_ware_house_row_id}"
            )

            # background_tasks.add_task(toss_req_uploading , data.azoo_product_id , 'request_premium_synthetic_uploading') # /request_premium_synthetic_uploading 을 호출한다.

            # /request_uploading 을 호출한다.
            background_tasks.add_task(
                toss_req_uploading,
                {
                    "product_type": "premium report",
                    "to_": "request_premium_report_uploading",
                    "azoo_product_id": data.azoo_product_id,
                    "premium_purchase_id": data.premium_purchase_id,
                    "sampling_table_id": data.sampling_table_row_id,
                },
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 product_warehouse 테이블을 insert 합니다.",
                "response_date": datetime.now(),
            }
            res = JSONResponse(content=jsonable_encoder(result), status_code=200)
            return res
        else:
            raise Exception("옳지 않은 요청입니다.")
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:{data} \n {traceback.format_exc()}"
        )
        logger.error(fail_message)

        result = {
            "end_point": end_point,
            "msg": "에러발생",
            "response_date": datetime.now(),
        }
        res = JSONResponse(content=jsonable_encoder(result), status_code=400)
        return res


async def toss_req_uploading(req_uploading_info: dict):
    """
    _ = requests.post(url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/azoo_product/request_uploading" , req_info=requst_uploading_req_info , headers=headers)
    """
    try:
        requst_uploading_req_info = None
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}

        if req_uploading_info["product_type"] == "common synthetic dataset":
            requst_uploading_req_info = {
                "azoo_product_id": req_uploading_info["azoo_product_id"]
            }

        elif req_uploading_info["product_type"] == "original dataset":
            requst_uploading_req_info = {
                "azoo_product_id": req_uploading_info["azoo_product_id"]
            }

        elif req_uploading_info["product_type"] == "premium synthetic dataset":
            requst_uploading_req_info = {
                "azoo_product_id": req_uploading_info["azoo_product_id"],
                "premium_purchase_id": req_uploading_info["premium_purchase_id"],
                "sampling_table_id": req_uploading_info["sampling_table_id"],
            }

        elif req_uploading_info["product_type"] == "premium report":
            requst_uploading_req_info = {
                "azoo_product_id": req_uploading_info["azoo_product_id"],
                "premium_purchase_id": req_uploading_info["premium_purchase_id"],
                "sampling_table_id": req_uploading_info["sampling_table_id"],
            }

        _ = requests.post(
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/azoo_product/{req_uploading_info['to_']}",
            json=requst_uploading_req_info,
            headers=headers,
        )
        logger.info(
            f"toss_req_uploading()에서 /{req_uploading_info['to_']} 를 호출했습니다...  => requst_uploading_req_info:{requst_uploading_req_info}"
        )
    except:
        logger.error(
            f"toss_req_uploading()에서 /{req_uploading_info['to_']} 를 호출도중 에러가 발생했습니다. ---> requst_uploading_req_info:{requst_uploading_req_info}"
        )
        logger.error(traceback.format_exc())


@router.post(
    path="/insert_original_product_warehouse",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_insert_original_product_warehouse(
    data: InsertWareHouseSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class InsertWareHouseSchema(BaseModel):  \n
        target_table:str = Query(...,description="table name")  \n
        azoo_product_id:int = Query(...,description="row id of azoo_product table")  \n
    """

    try:
        end_point = f"{router.prefix}/api_insert_original_product_warehouse"

        if data.target_table == "product_warehouse":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            await insert_product_warehouse_table_of_original_dataset(
                db=db, azoo_product_id=data.azoo_product_id, status="Pending"
            )
            logger.info(
                f"data:{data} 로 [Original] product_warehouse 테이블을 insert 합니다."
            )

            # /request_uploading 을 호출한다.
            background_tasks.add_task(
                toss_req_uploading,
                {
                    "product_type": "original dataset",
                    "to_": "request_original_product_uploading",
                    "azoo_product_id": data.azoo_product_id,
                },
            )

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 product_warehouse 테이블을 insert 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
    path="/update_product_warehouse",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_product_warehouse(
    data: UpdateWareHouseSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UpdateWareHouseSchema(BaseModel): \n
        target_table:str = Query(...,description="table name") \n
        product_warehouse_row_id:int = Query(...,description="row id of product_warehouse table") \n
        status_to:str = Query(...,description="wanna status to this") \n
        s3_key:str|None = Query(...,description="s3 object_key") \n
        purchase_id:int|None = Query(...,description="row id of purchase table") \n
        premium_generated_at:datetime|None = Query(default=None,description="generated at == datetime.now()") \n
    """

    try:
        end_point = f"{router.prefix}/update_product_warehouse"

        if data.target_table == "product_warehouse":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            await update_product_warehouse_table_status(
                db=db,
                product_warehouse_row_id=data.product_warehouse_row_id,
                status_to=data.status_to,
                s3_key=data.s3_key,
                premium_generated_at=data.premium_generated_at,
            )
            logger.info(f"data:{data} 로 product_warehouse 테이블을 update 합니다.")

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 product_warehouse 테이블을 update 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
    path="/update_purchase_status",
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_purchase_status(
    data: UpdatePurchaseSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UpdatePurchaseSchema(BaseModel):  \n
        purchase_id:int = Query(...,description="row id of purchase table") \n
        status_to:str = Query(...,description="wanna status to this") \n
    """

    try:
        # update_purchase_table_status

        end_point = f"{router.prefix}/update_purchase_status"
        logger.info(
            f"{end_point} 로 요청이 들어왔습니다. product_warehouse 테이블을 update 합니다. --> {data}"
        )

        _ = await update_purchase_table_status(
            db=db, purchase_row_id=data.purchase_id, status_to=data.status_to
        )
        result = {
            "end_point": end_point,
            "result": f"data:{data} 로 product_warehouse 테이블을 update 합니다.",
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
    path="/noti_purchased",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_noti_purchased(
    data: NotiPurchasedSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    Common Product 의 거래가 이루어 지면, 호출되는 api 이다. \n
    이 api 에 의해서, s3에 있는 재고들 중에서, purchase_id 에 해당되는 product 를 매칭시켜준다. \n
    이를 통해서, purchase_id 는 s3에 있는 상품과 매칭이 이뤄지며.. 매칭이 완료되면, buyer 는 해당 상품의 소유주가 된다. ---> download 가능해짐

    class NotiPurchasedSchema(BaseModel):    \n
        purchase_id:int = Query(...,description="row id of purchase table") \n
    """

    try:
        end_point = f"{router.prefix}/noti_purchased"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        sold_at, product_warehouse_row_id, azoo_product_id = (
            await get_product_warehouse_row_id(
                db=db, purchase_id=data.purchase_id, because_of="noti_purchased"
            )
        )
        status_to = "Sold"
        await update_product_warehouse_table_status(
            db=db,
            product_warehouse_row_id=product_warehouse_row_id,
            status_to=status_to,
            purchase_id=data.purchase_id,
            sold_at=sold_at,
        )

        update_info = {
            "purchase_id": data.purchase_id,
            "product_warehouse_row_id": product_warehouse_row_id,
            "status_to": status_to,
            "sold_at": sold_at,
        }
        logger.info(f"data:{update_info} 로 product_warehouse 테이블을 update 합니다.")

        result = {
            "end_point": end_point,
            "result": f"data:{update_info} 로 product_warehouse 테이블을 update 합니다.",
            "response_date": datetime.now(),
        }
        pass

        ### API 호출하면 된다. --> 백그라운드로 실행하면 된다.
        azoo_api_req_info = {"azoo_product_id": azoo_product_id}
        logger.info(
            f"백그라운드 워커로 call_azoo_api 호출합니다. req_info:{azoo_api_req_info}"
        )
        background_tasks.add_task(
            call_azoo_api,
            method="post",
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/azoo_product/request_prepare_products",
            req_info=azoo_api_req_info,
        )
        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params(purchase_id):{NotiPurchasedSchema.purchase_id} \n {traceback.format_exc()}"
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
    path="/noti_premium_purchased",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_noti_premium_purchased(
    data: NotiPurchasedSchema,
    background_tasks: BackgroundTasks,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class NotiPurchasedSchema(BaseModel):    \n
        purchase_id:int = Query(...,description="row id of purchase table") \n
    """

    try:
        end_point = f"{router.prefix}/noti_premium_purchased"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        ### 모든 premium_product의 경우는, 무조건 generating_synthetic_tasks 테이블에 해당 제품의 생성 요청을 추가하도록 한다.
        dataset_id_dict = await get_dataset_id_by_purchase_id(
            db=db, purchase_id=data.purchase_id
        )  # purchase_id 이용해서, product_registration 테이블에서, dataset_id 가져옴
        logger.info(
            f"Purchase_id: {data.purchase_id} 에 해당되는 dataset_id 는 {dataset_id_dict} 입니다."
        )

        if (
            report_product := dataset_id_dict.get("report_product", None)
        ) is not None:  # dataset_id_dict["report_product"] = (str(report_product_dataset_id) , report_product_quantity)
            # Premium Report 생성해야 함
            azoo_api_req_info = {
                "project_id": 5,  # Premium-Report --> projects 테이블을 보면 됨
                "project_name": "Premium Report 생성",
                "dataset_id": report_product[0],
                "ai_model_id": "018eea81-1421-797f-985c-7dbd03826dad",  ## 고정값. Michael 서버에서 오류 없기위해 사용함 018eea81-1421-797f-985c-7dbd03826dad
                "premium_purchase_id": data.purchase_id,
            }
            logger.info(
                f"백그라운드 워커로 call_azoo_api 호출합니다. req_info:{azoo_api_req_info} --> /cure/new_task"
            )
            background_tasks.add_task(
                call_azoo_api,
                method="post",
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/cure/new_task",
                req_info=azoo_api_req_info,
            )
            pass

        if (
            dataset_product := dataset_id_dict.get("common_dataset_product", None)
        ) is not None:  # dataset_id_dict["common_dataset_product"] = (str(dataset_product_dataset_id) , dataset_product_quantity)
            # Common Synthetic Dataset 의 판매를 처리해야 함. --> /noti_purchased 호출
            azoo_api_req_info = {"purchase_id": data.purchase_id}
            logger.info(
                f"백그라운드 워커로 call_azoo_api 호출합니다. req_info:{azoo_api_req_info} --> /core/noti_purchased"
            )
            background_tasks.add_task(
                call_azoo_api,
                method="post",
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/noti_purchased",
                req_info=azoo_api_req_info,
            )

            azoo_api_req_info = {
                "purchase_id": data.purchase_id,
                "status_to": "Premium Dataset Active",  # Common Dataset + Premium Report 의 조합은.. Common Dataset 은 바로 download가 가능하므로, purchase의 상태값을 4. Premium Dataset Active 으로 바로 설정한다.
            }
            logger.info(
                f"백그라운드 워커로 call_azoo_api 호출합니다. req_info:{azoo_api_req_info} --> /core/update_purchase_status "
            )
            background_tasks.add_task(
                call_azoo_api,
                method="post",
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_purchase_status",
                req_info=azoo_api_req_info,
            )

            result = {
                "end_point": end_point,
                "result": f"data:{azoo_api_req_info} 로 /core/noti_purchased 를 호출하였습니다. ---> Common Synthetic Dataset 의 판매를 처리합니다.",
                "response_date": datetime.now(),
            }

        elif (
            dataset_product := dataset_id_dict.get("premium_dataset_product", None)
        ) is not None:  # dataset_id_dict["premium_dataset_product"] = (str(dataset_product_dataset_id) , dataset_product_quantity)
            # Premium Dataset 생성해야 함
            azoo_api_req_info = {
                "project_id": 4,  # Premium synthetic Dataset --> projects 테이블을 보면 됨
                "project_name": "Premium Dataset 생성",
                "dataset_id": dataset_product[0],
                "ai_model_id": "018eea81-1421-797f-985c-7dbd03826dad",  ## 고정값. Michael 서버에서 오류 없기위해 사용함 018eea81-1421-797f-985c-7dbd03826dad
                "premium_purchase_id": data.purchase_id,
            }
            logger.info(
                f"백그라운드 워커로 call_azoo_api 호출합니다. req_info:{azoo_api_req_info} --> /cure/new_task"
            )
            background_tasks.add_task(
                call_azoo_api,
                method="post",
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/cure/new_task",
                req_info=azoo_api_req_info,
            )

            result = {
                "end_point": end_point,
                "result": f"data:{azoo_api_req_info} 로 /cure/new_task 를 호출하였습니다. ---> 새로운 synthetic task 가 생성됩니다.",
                "response_date": datetime.now(),
            }

        res = JSONResponse(content=jsonable_encoder(result), status_code=200)
        return res
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params(purchase_id):{NotiPurchasedSchema.purchase_id} \n {traceback.format_exc()}"
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
    path="/noti_canceled",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_noti_canceled(
    data: NotiCanceledSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class NotiCanceledSchema(BaseModel):    \n
        purchase_id:int = Query(...,description="row id of purchase table") \n
    """

    try:
        end_point = f"{router.prefix}/noti_canceled"
        logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

        product_warehouse_row_ids = await get_product_warehouse_row_id(
            db=db, purchase_id=data.purchase_id, because_of="noti_canceled"
        )
        status_to = "Cancelled"
        await update_product_warehouse_table_status(
            db=db,
            product_warehouse_row_id=product_warehouse_row_ids,
            status_to=status_to,
        )

        result = {
            "end_point": end_point,
            "result": f"product_warehouse_row_ids:{product_warehouse_row_ids} 를 status:Cancelled 로 update 합니다.",
            "response_date": datetime.now(),
        }
        pass

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
    path="/update_azoo_product",  # Done
    tags=["Core"],
    responses={
        200: {"description": "API 호출 성공", "model": ReturnResultSchema},
        400: {
            "description": "API 호출 과정에서 오류발생",
            "model": ReturnMessageSchema,
        },
    },
)
async def api_update_azoo_product(
    data: UpdateAzooProductSchema,
    db=Depends(async_get_db),
    access_token=Depends(JWTBearer()),
):  # access_token = Depends(JWTBearer())
    """
    class UpdateAzooProductSchema(BaseModel): \n
        target_table:str = Query(...,description="table name") \n
        azoo_product_row_id:int = Query(...,description="row id of azoo_product table") \n
        status_to:str = Query(...,description="wanna status to this") \n
    """

    try:
        end_point = f"{router.prefix}/update_azoo_product"

        if data.target_table == "azoo_product":
            logger.info(f"{end_point} 로 요청이 들어왔습니다. --> {data}")

            await update_azoo_product_table_status(
                db=db,
                azoo_product_row_id=data.azoo_product_row_id,
                status_to=data.status_to,
            )
            logger.info(f"data:{data} 로 azoo_product 테이블을 update 합니다.")

            result = {
                "end_point": end_point,
                "result": f"data:{data} 로 azoo_product 테이블을 update 합니다.",
                "response_date": datetime.now(),
            }
            pass

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
