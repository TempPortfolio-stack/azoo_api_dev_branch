# S01-IM-1001-0001-CUST_REQ_ATP_DTA-0006-8735 : wav GID 운영
import time
import traceback
from datetime import datetime

from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from Common.jwt import JWTBearer
from Model.models import GeneratingSyntheticTasksModel
from Model.schema import (
    DbconnectionListSchema,
    ReturnResultSchema,
    GenericPageSchema,
)
from Settings.Database.database import get_db
from Settings.Logger.logging_config import fastapi_logger

# from fastapi_pagination import Page , Params , LimitOffsetParams , LimitOffsetPage , paginate

router = APIRouter(
    prefix="/cure",
)


@router.get(
    path="/running_tasks_list",
    tags=["문서 개요 3.10 : /running_tasks_list "],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": GenericPageSchema[DbconnectionListSchema],
        },
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
    },
)
async def api_ruinning_tasks_list(
    offset=1, limit=50, db=Depends(get_db), access_token=Depends(JWTBearer())
):
    """
    Admin 이 curedata 생성할때, DB 커넥션 정보 입력하는 팝업에 보여주는, 현재 생성중인 curedata task 목록들 보여주는 API
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/running_tasks_list"

        with db.begin():  ### 트랜젝션
            task_list = (
                db.query(GeneratingSyntheticTasksModel)
                .filter(
                    GeneratingSyntheticTasksModel.task_status_id.in_([1, 2])
                    & GeneratingSyntheticTasksModel.is_deleted.is_(False)
                )
                .order_by(GeneratingSyntheticTasksModel.created_at.asc())
                .offset(offset)
                .limit(limit)
                .all()
            )

            total_table_items = (
                db.query(GeneratingSyntheticTasksModel)
                .filter(
                    GeneratingSyntheticTasksModel.task_status_id.in_([1, 2])
                    & GeneratingSyntheticTasksModel.is_deleted.is_(False)
                )
                .count()
            )

            schmed_task_list = []
            for task in task_list:
                try:
                    if task.product_registration_row is not None:
                        id = task.id
                        dataset_id = task.dataset_id
                        created_at = (
                            task.created_at if task.created_at is not None else "None"
                        )
                        data_format = (
                            task.product_registration_row.data_format.name
                            if task.product_registration_row.data_format is not None
                            else "None"
                        )
                        data_type = (
                            task.product_registration_row.data_type.name
                            if task.product_registration_row.data_type is not None
                            else "None"
                        )
                        admin_name = (
                            task.product_registration_row.contract.admin.username
                            if task.product_registration_row.contract.admin is not None
                            else "None"
                        )

                        schmed_task_list.append(
                            DbconnectionListSchema(
                                id=id,
                                created_at=created_at,
                                data_format=data_format,
                                data_type=data_type,
                                admin_name=admin_name,
                                dataset_id=dataset_id,
                            )
                        )

                        fastapi_logger.info(
                            f"id : {id} , dataset_id:{dataset_id} , created_at:{created_at} , data_format:{data_format} , data_type:{data_type} , admin_name:{admin_name}"
                        )
                except:
                    fastapi_logger.error(traceback.format_exc())
                    continue
                pass
            pass

            # schmed_task_list = [ TaskSchema.model_validate(task) for task in task_list]

        end = time.time()

        length_of_items = len(schmed_task_list)

        result = {
            "end_point": end_point,
            "items": schmed_task_list,
            "length_of_items": length_of_items,
            "working_time_sec": end - begin,
            "total_table_items": total_table_items,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(f"{result}")
        return result
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:None \n {traceback.format_exc()}"
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


@router.get(
    path="/tasks_list",
    tags=["문서 개요 3.15 /tasks_list"],
    responses={
        200: {
            "description": "API 호출 성공",
            "model": GenericPageSchema[DbconnectionListSchema],
        },
        400: {"description": "API 호출 과정에서 오류발생", "model": ReturnResultSchema},
    },
)
async def api_ruinning_tasks_list_page(
    offset: int = 0,
    limit: int = 50,
    db=Depends(get_db),
    access_token=Depends(JWTBearer()),
):
    """
    Admin 이 curedata 생성할때, DB 커넥션 정보 입력하는 팝업에 보여주는, 현재 생성중인 curedata task 목록들 보여주는 API
    """

    try:
        begin = time.time()
        end_point = f"{router.prefix}/tasks_list"
        task_list = (
            db.query(GeneratingSyntheticTasksModel)
            .filter(
                GeneratingSyntheticTasksModel.task_status_id.not_in([3, 5])
                & GeneratingSyntheticTasksModel.is_deleted.is_(False)
            )
            .order_by(GeneratingSyntheticTasksModel.created_at.asc())
            .offset(offset)
            .limit(limit)
            .all()
        )
        total_table_items = (
            db.query(GeneratingSyntheticTasksModel)
            .filter(
                GeneratingSyntheticTasksModel.task_status_id.not_in([3, 5])
                & GeneratingSyntheticTasksModel.is_deleted.is_(False)
            )
            .count()
        )
        end = time.time()

        schmed_task_list = []
        for task in task_list:
            try:
                if task.product_registration_row is not None:
                    id = task.id
                    dataset_id = task.dataset_id
                    created_at = (
                        task.created_at if task.created_at is not None else "None"
                    )
                    data_format = (
                        task.product_registration_row.data_format.name
                        if task.product_registration_row.data_format is not None
                        else "None"
                    )
                    data_type = (
                        task.product_registration_row.data_type.name
                        if task.product_registration_row.data_type is not None
                        else "None"
                    )
                    admin_name = (
                        task.product_registration_row.contract.admin.username
                        if task.product_registration_row.contract.admin is not None
                        else "None"
                    )

                    schmed_task_list.append(
                        DbconnectionListSchema(
                            id=id,
                            created_at=created_at,
                            data_format=data_format,
                            data_type=data_type,
                            admin_name=admin_name,
                            dataset_id=dataset_id,
                        )
                    )

                    fastapi_logger.info(
                        f"id : {id} , dataset_id:{dataset_id} , created_at:{created_at} , data_format:{data_format} , data_type:{data_type} , admin_name:{admin_name}"
                    )
            except:
                print(traceback.format_exc())
                continue
            pass

        # schmed_task_list = [ TaskSchema.model_validate(task) for task in task_list]
        length_of_items = len(schmed_task_list)

        result = {
            "end_point": end_point,
            "items": schmed_task_list,
            "length_of_items": length_of_items,
            "working_time_sec": end - begin,
            "total_table_items": total_table_items,
            "response_date": datetime.now(),
        }
        fastapi_logger.info(f"{result}")
        return result
    except Exception as e:
        fail_message = (
            f"Endpoint:{end_point} \n Params:None \n {traceback.format_exc()}"
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
