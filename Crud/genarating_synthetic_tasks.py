import traceback
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from Common.util import generate_uuid
from Model.new_models import (
    MgeneratingSyntheticTaskStatus,
    GeneratingSyntheticTasksModel,
    ProductRegistrationModel,
    AzooProductModel,
    OrderModel,
    PurchaseModel,
    ProductBaseInfoModel,
)
from Settings.Logger.logging_config import db_util_logger as logger


async def update(db, params: dict):
    try:
        logger.info(
            f"genarating_synthetic_tasks.py 의 update()에 작업 요청이 들어왔습니다. --> params:{params}"
        )

        if params["from"] == "api_noti_task_done()_premium_report":
            result = await db.execute(
                select(GeneratingSyntheticTasksModel).where(
                    GeneratingSyntheticTasksModel.id == params["synthetic_table_id"]
                )
            )
            synthetic_task_row = result.scalar()
            logger.info(f"{synthetic_task_row.id} 입니다.")
            synthetic_task_row.updated_at = datetime.now()
            synthetic_task_row.dataset_report_json = params["json_report"]
            db.add(synthetic_task_row)
            await db.commit()

        elif params["from"] == "toss_new_task()":
            async with db.begin():
                new_row_id = params["new_row_id"]
                task_id = generate_uuid(
                    params["dataset_id"]
                )  # 걍, dataset_path 는 unique하니까, 이걸 seed로 task_id 생성함

                subquery_product_regisration = (
                    select(ProductRegistrationModel)
                    .where(ProductRegistrationModel.dataset_id == params["dataset_id"])
                    .subquery()
                )

                result = await db.execute(
                    select(GeneratingSyntheticTasksModel).where(
                        GeneratingSyntheticTasksModel.id == new_row_id
                    )
                )

                synthetic_task_row = result.scalar()
                synthetic_task_row.task_id = task_id
                synthetic_task_row.product_registration_id = (
                    subquery_product_regisration.c.id
                )
                synthetic_task_row.updated_at = datetime.now()
                db.add(synthetic_task_row)
                await db.commit()
            return task_id
        pass
    except:
        logger.error(traceback.format_exc())
        raise


async def create(db, params: dict):
    try:
        logger.info(
            f"genarating_synthetic_tasks.py 의 create()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        if params["from"] == "api_new_task()":
            async with db.begin():  ### 트랜젝션
                req_params = params["req_params"]
                ai_model_id = req_params["ai_model_id"]
                dataset_id = req_params["dataset_id"]
                project_id = req_params["project_id"]
                project_name = req_params["project_name"]

                new_row = GeneratingSyntheticTasksModel()
                new_row.updated_at = func.now()
                new_row.created_at = func.now()
                new_row.is_deleted = False
                new_row.task_status_id = 1  # 1: Pending
                new_row.dataset_score = "--"
                # new_row.dataset_report_html_file_url = "--"

                new_row.ai_model_id = ai_model_id
                new_row.dataset_id = dataset_id
                new_row.project_id = project_id
                new_row.project_name = project_name
                new_row.premium_purchase_id = (
                    int(req_params["premium_purchase_id"])
                    if req_params["premium_purchase_id"] is not None
                    else None
                )

                subquery_product_registration_row = (
                    select(ProductRegistrationModel.id)
                    .where(ProductRegistrationModel.dataset_id == new_row.dataset_id)
                    .subquery()
                )
                new_row.product_registration_id = subquery_product_registration_row
                db.add(new_row)
                pass  # 트랜젝션 종료
            await db.refresh(new_row)
            return new_row.id
    except:
        logger.error(traceback.format_exc())
        raise


async def read(db, params: dict):
    try:
        logger.info(
            f"genarating_synthetic_tasks.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        async with db.begin():  ### 트랜젝션
            if params["from"] == "api_get_syndata()":                
                sub_query = select(GeneratingSyntheticTasksModel.premium_purchase_id)\
                            .where(GeneratingSyntheticTasksModel.task_id == params["task_id"])
                
                query = (
                    select(GeneratingSyntheticTasksModel)
                    .options(
                        selectinload(
                            GeneratingSyntheticTasksModel.product_registration_row
                        )
                        .selectinload(ProductRegistrationModel.product_base_info_row)
                        .selectinload(ProductBaseInfoModel.data_format)
                    )
                    .where(
                        (GeneratingSyntheticTasksModel.premium_purchase_id == sub_query)                        
                    )
                )
                result = await db.execute(query)
                result_rows = result.scalars()

                generating_synthetic_row = None
                for synthetic_task_row in result_rows:
                    if synthetic_task_row.dataset_report_json is None:
                        continue
                    generating_synthetic_row = synthetic_task_row
                    pass
                
                data_format = (
                                generating_synthetic_row.product_registration_row.product_base_info_row.data_format.name
                            )
                report = generating_synthetic_row.dataset_report_json
                return (data_format, report)
            
                # query = (
                #     select(GeneratingSyntheticTasksModel)
                #     .options(
                #         selectinload(
                #             GeneratingSyntheticTasksModel.product_registration_row
                #         )
                #         .selectinload(ProductRegistrationModel.product_base_info_row)
                #         .selectinload(ProductBaseInfoModel.data_format)
                #     )
                #     .where(
                #         (GeneratingSyntheticTasksModel.task_id == params["task_id"]),
                #         (GeneratingSyntheticTasksModel.dataset_report_json != None),
                #     )
                # )
                # result = await db.execute(query)
                # generating_synthetic_row = result.scalar()
                # if not generating_synthetic_row:
                #     raise Exception(
                #         f"There are no generating synthetic tasks for this task id: {params['task_id']}."
                #     )

                # data_format = (
                #     generating_synthetic_row.product_registration_row.product_base_info_row.data_format.name
                # )
                # report = generating_synthetic_row.dataset_report_json
                # # report = result.scalar()
                # return (data_format, report)

            elif params["from"] == "api_request_prepare_products()":
                product_registration_row_id = params["product_registration_row_id"]
                synthetic_task_id = params["synthetic_tasks_id"]

                sub_query = (
                    select(MgeneratingSyntheticTaskStatus.id)
                    .where(
                        func.lower(MgeneratingSyntheticTaskStatus.name)
                        == "INSERTED_TO_DB".lower()
                    )
                    .subquery()
                )
                query = select(GeneratingSyntheticTasksModel , PurchaseModel , OrderModel)\
                    .join(PurchaseModel ,
                        PurchaseModel.id == GeneratingSyntheticTasksModel.premium_purchase_id)\
                    .join(OrderModel , 
                        OrderModel.id == PurchaseModel.order_id)\
                    .where(
                    (
                            GeneratingSyntheticTasksModel.task_id
                            == synthetic_task_id
                    ),
                    (GeneratingSyntheticTasksModel.is_deleted == False),
                    (GeneratingSyntheticTasksModel.task_status_id == sub_query),
                )
                result = await db.execute(query)
                generating_synthetic_task_model , purchase_model , order_model = result.fetchone()
                logger.info(
                    f"[Result] synthetic_task_id : {generating_synthetic_task_model.task_id} , premium_purchase_id : {generating_synthetic_task_model.premium_purchase_id} , purchase_id : {purchase_model.id} , order_id : {purchase_model.order_id} , order_id : {order_model.id} , dataset_product_id : {order_model.dataset_product_id} , dataset_product_quantity: {order_model.dataset_product_quantity} , report_product_id: {order_model.report_product_id} , report_product_quantity : {order_model.report_product_quantity}"
                )
                return generating_synthetic_task_model.task_id , order_model.dataset_product_quantity
                

                ##### Legacy 코드
                # query = select(GeneratingSyntheticTasksModel.task_id).where(
                #     (
                #         GeneratingSyntheticTasksModel.product_registration_id
                #         == product_registration_row_id
                #     ),
                #     (GeneratingSyntheticTasksModel.is_deleted == False),
                #     (GeneratingSyntheticTasksModel.task_status_id == sub_query),
                # )
                # result = await db.execute(query)
                # generaitng_synthetic_task_id = result.scalar()
                # logger.info(
                #     f"[Return] generaitng_synthetic_task_id:{generaitng_synthetic_task_id}"
                # )
                # return generaitng_synthetic_task_id

            elif params["from"] == "api_get_complete_list()":
                subquery_premium_report_purchase_id = (
                    select(PurchaseModel.id)
                    .select_from(PurchaseModel)
                    .join(OrderModel, PurchaseModel.order_id == OrderModel.id)
                    .where(
                        (PurchaseModel.user_id == params["user_id"]),
                        (OrderModel.report_product_id != None),
                    )
                    .subquery()
                )

                query = (
                    select(GeneratingSyntheticTasksModel)
                    .options(
                        selectinload(
                            GeneratingSyntheticTasksModel.product_registration_row
                        )
                        .selectinload(ProductRegistrationModel.product_base_info_row)
                        .selectinload(ProductBaseInfoModel.data_format)
                    )
                    .where(
                        (
                            GeneratingSyntheticTasksModel.premium_purchase_id.in_(
                                select(subquery_premium_report_purchase_id)
                            )
                        ),
                        (
                            GeneratingSyntheticTasksModel.task_status_id == 7
                        ),  # Inserted to DB
                    )
                )
                query_result = await db.execute(query)
                rows = query_result.all()

                if rows is None or len(rows) == 0:
                    result = []
                else:
                    result = [
                        {
                            "task_id": row[0].task_id,
                            "dataset_name": row[
                                0
                            ].product_registration_row.product_base_info_row.name,
                            "dataset_type": row[
                                0
                            ].product_registration_row.product_base_info_row.data_format.name.lower(),
                        }
                        for row in rows
                    ]
                return result

            elif params["from"] in ["api_get_eval_and_thres()", "api_get_evaluation()"]:
                azoo_product_id = params["azoo_product_id"]

                query = (
                    select(
                        GeneratingSyntheticTasksModel,
                        AzooProductModel,
                        ProductRegistrationModel,
                    )
                    .join(
                        AzooProductModel,
                        GeneratingSyntheticTasksModel.dataset_id
                        == AzooProductModel.dataset_id,
                    )
                    .join(
                        ProductRegistrationModel,
                        GeneratingSyntheticTasksModel.dataset_id
                        == ProductRegistrationModel.dataset_id,
                    )
                    .options(
                        selectinload(
                            ProductRegistrationModel.product_base_info_row
                        ).selectinload(ProductBaseInfoModel.data_format)
                    )
                    .where(
                        (AzooProductModel.id == azoo_product_id),
                        (GeneratingSyntheticTasksModel.task_status_id == 7),
                    )
                )

                query_result = await db.execute(query)
                result = query_result.all()
                if len(result) == 0:
                    return (None, None)
                (
                    generating_synthetic_task_row,
                    azoo_product_row,
                    product_registration_row,
                ) = result[0]
                data_type = (
                    product_registration_row.product_base_info_row.data_format.name
                )
                return (data_type, generating_synthetic_task_row.dataset_report_json)

            elif params["from"] == "api_random_sampling()":
                synthetic_task_id = params["synthetic_task_id"]
                sub_query = (
                    select(MgeneratingSyntheticTaskStatus.id)
                    .where(
                        func.lower(MgeneratingSyntheticTaskStatus.name)
                        == "INSERTED_TO_DB".lower()
                    )
                    .subquery()
                )
                query = (
                    select(GeneratingSyntheticTasksModel)
                    .options(
                        selectinload(
                            GeneratingSyntheticTasksModel.product_registration_row
                        )
                        .selectinload(ProductRegistrationModel.azoo_product)
                        .selectinload(AzooProductModel.dataset_product_order_row),
                        selectinload(
                            GeneratingSyntheticTasksModel.premium_purchase_row
                        ).selectinload(PurchaseModel.order_row),
                    )
                    .filter(
                        (GeneratingSyntheticTasksModel.task_status_id == sub_query)
                        & (GeneratingSyntheticTasksModel.is_deleted == False)
                        & (GeneratingSyntheticTasksModel.task_id == synthetic_task_id)
                    )
                    .order_by(GeneratingSyntheticTasksModel.created_at.desc())
                )

                res = await db.execute(query)
                task = res.unique().scalar()
                if task.product_registration_row.azoo_product is None or len(task.product_registration_row.azoo_product) == 0:
                    logger.warning(f"AZOO Product 에 등록되어 있지 않은 상품입니다. 관리자 페이지에서, AZOO Product 로 등록을 미리 해 주세요. --> params:{params}")
                    return None
                else:
                    logger.info(f"azoo_product : {task.product_registration_row.azoo_product} 의 랜덤샘플링 작업이 진행됩니다.")

                req_info = {
                    "synthetic_project_id": task.project_id,
                    "synthetic_task_row_id": task.id,
                    "synthetic_task_id": synthetic_task_id,
                    "product_registration_id": task.product_registration_row.id,
                    "pkl_path": task.result_files_list_pkl_path,
                    "nums_of_buffer_sampling": task.product_registration_row.nums_of_buffer_sampling,
                }

                if task.project_id in [1, 2, 3]:                    
                    if task.product_registration_row.azoo_product[0].default_dataset_quantity != task.premium_purchase_row.order_row.dataset_product_quantity:
                        # Multiple Common Synthetic Dataset --> Premium Dataset 취급한다.
                        req_info["dataset_product_quantity"] = (
                            task.premium_purchase_row.order_row.dataset_product_quantity
                        )
                        req_info["synthetic_project_id"] = 4 # Premium 취급시킨다.
                        pass
                    else:
                        # Common Synthetic Dataset -->
                        req_info["dataset_product_quantity"] = (
                            task.product_registration_row.azoo_product[
                                0
                            ].default_dataset_quantity
                        )
                    pass
                elif task.project_id == 4:
                    # Premium Synthetic Dataset -->  이 경우는, 미리 판매를 해서, order table에서 dataset_product_quantity 를 얻을 수 있는 --> Premium Dataset Product 의 경우이다.
                    req_info["dataset_product_quantity"] = (
                        task.premium_purchase_row.order_row.dataset_product_quantity
                    )
                    # req_info['dataset_product_quantity'] = task.product_registration_row.azoo_product[0].dataset_product_order_row[-1].dataset_product_quantity
                    pass
                # elif task.project_id == 5:
                #     # Premium Report
                #     pass
                logger.info(f"[Return] req_info:{req_info}")
                return req_info
    except:
        logger.error(traceback.format_exc())
        raise
