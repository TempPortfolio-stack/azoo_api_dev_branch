import traceback
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from Common.redis_token import RedisClient
from Model.new_models import (
    SamplingModel,
    MSamplingStatusModel,
    ProductRegistrationModel,
    PurchaseModel,
    GeneratingSyntheticTasksModel,
)
from Settings.Logger.logging_config import db_util_logger as logger


async def create(db, params: dict):
    try:
        logger.info(
            f"sampling.py 의 create()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        if params["from"] == "api_noti_task_done()_premium_report":
            async with db.begin():  ### 트랜젝션
                #### exist_row --> Premium Report 의 경우는, sampling 테이블에서 1개의 row 만 가지는게 정상
                # query = select(SamplingModel).where(SamplingModel.generaitng_synthetic_task_id == params['generating_synthetic_task_row_id'])
                # result = await db.execute(query)
                # exist_row = result.scalar()
                # if exist_row is not None:
                #     return exist_row.id

                new_row = SamplingModel()
                new_row.created_at = datetime.now()
                new_row.updated_at = datetime.now()
                new_row.is_deleted = False
                new_row.random_sampled_list = "[Premium Report]"
                new_row.product_registration_id = params["product_registration_row_id"]
                new_row.status_id = 1  # sampled
                new_row.generaitng_synthetic_task_id = params[
                    "generating_synthetic_task_row_id"
                ]
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
            f"sampling.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        async with db.begin():  ### 트랜젝션

            if params["from"] == "api_request_packaging_for_Premium_Report()":
                product_registration_id = params["product_registration_id"]
                synthetic_task_id = params["synthetic_task_id"]

                # subquery_premium_purchase_id = select(GeneratingSyntheticTasksModel.premium_purchase_id).where(GeneratingSyntheticTasksModel.task_id == synthetic_task_id).subquery()
                subquery_synthetic_task_row = (
                    select(GeneratingSyntheticTasksModel)
                    .where(GeneratingSyntheticTasksModel.task_id == synthetic_task_id)
                    .subquery()
                )
                query = (
                    select(PurchaseModel)
                    .options(selectinload(PurchaseModel.order_row))
                    .where(
                        PurchaseModel.id
                        == subquery_synthetic_task_row.c.premium_purchase_id
                    )
                )
                result = await db.execute(query)
                purchase_row = result.scalar()

                premium_report_azoo_product_id = (
                    purchase_row.order_row.report_product_id
                )
                premium_report_quantity = purchase_row.order_row.report_product_quantity

                subquery_sampling_status = (
                    select(MSamplingStatusModel.id)
                    .where(func.lower(MSamplingStatusModel.name) == "Packaged".lower())
                    .subquery()
                )
                query = (
                    select(SamplingModel)
                    .options(
                        selectinload(SamplingModel.synthetic_task_row),
                        selectinload(
                            SamplingModel.product_registration_row
                        ).selectinload(ProductRegistrationModel.azoo_product),
                    )
                    .where(
                        (
                            SamplingModel.generaitng_synthetic_task_id
                            == subquery_synthetic_task_row.c.id
                        )
                        # (SamplingModel.len_sampled_list == premium_dataset_quantity) # len_sampled_list 와 premium_purchase_id 의 판매정보와 동일해야 한다.
                    )
                )

                result = await db.execute(query)
                sampling_row = result.scalar()

                result_dict = {
                    "azoo_product_id": premium_report_azoo_product_id,
                    "premium_purchase_id": purchase_row.id,
                    "product_type_id": sampling_row.product_registration_row.product_type_id,  # 1:Common Dataset / 2:Premium Dataset / 3:Premium-Report
                    "sampling_table_row_id": sampling_row.id,
                    "product_registration_id": product_registration_id,
                    "random_sampled_list": sampling_row.random_sampled_list,
                    "pkl_path": sampling_row.synthetic_task_row.result_files_list_pkl_path,
                    "original_dataset_path": sampling_row.product_registration_row.dataset_path,
                    "target_cnt_of_zipfiles": 1,  # premium_report 경우는 매번 새롭게 1개의 상품만 만들것이므로 target은 1로 한다.
                    "current_cnt_of_zip_files": 0,  # premium_report 경우는 매번 새롭게 1개의 상품만 만들것이므로
                    "synthetic_task_id": sampling_row.synthetic_task_row.task_id,
                    "result_dataset_path": sampling_row.synthetic_task_row.result_dataset_path,
                }

                return [
                    result_dict
                ]  # Premium Report는 패키징 작업에 item 이 1개인 list를 반환한다.

            elif (
                params["from"]
                == "api_request_packaging_for_Premium_Synthetic_Dataset()"
            ):
                product_registration_id = params["product_registration_id"]
                synthetic_task_row_id = params["synthetic_task_row_id"]

                subquery_premium_purchase_id = (
                    select(GeneratingSyntheticTasksModel.premium_purchase_id)
                    .where(GeneratingSyntheticTasksModel.id == synthetic_task_row_id)
                    .subquery()
                )
                query = (
                    select(PurchaseModel)
                    .options(selectinload(PurchaseModel.order_row))
                    .where(PurchaseModel.id == subquery_premium_purchase_id)
                )
                result = await db.execute(query)
                purchase_row = result.scalar()

                premium_dataset_azoo_product_id = (
                    purchase_row.order_row.dataset_product_id
                )
                premium_dataset_quantity = (
                    purchase_row.order_row.dataset_product_quantity
                )

                subquery_sampling_status = (
                    select(MSamplingStatusModel.id)
                    .where(func.lower(MSamplingStatusModel.name) == "Sampled".lower())
                    .subquery()
                )
                query = (
                    select(SamplingModel)
                    .options(
                        selectinload(SamplingModel.synthetic_task_row),
                        selectinload(
                            SamplingModel.product_registration_row
                        ).selectinload(ProductRegistrationModel.azoo_product),
                    )
                    .where(
                        (
                            SamplingModel.generaitng_synthetic_task_id
                            == synthetic_task_row_id
                        ),
                        (SamplingModel.status_id == subquery_sampling_status),
                        (SamplingModel.id == params["new_sampling_row_id"]),
                    )
                )

                result = await db.execute(query)
                sampling_row = result.scalar()

                result_dict = {
                    "azoo_product_id": premium_dataset_azoo_product_id,
                    "premium_purchase_id": purchase_row.id,
                    "product_type_id": sampling_row.product_registration_row.product_type_id,  # 1:Common Dataset / 2:Premium Dataset / 3:Premium-Report
                    "sampling_table_row_id": sampling_row.id,
                    "product_registration_id": product_registration_id,
                    "random_sampled_list": sampling_row.random_sampled_list,
                    "pkl_path": sampling_row.synthetic_task_row.result_files_list_pkl_path,
                    "original_dataset_path": sampling_row.product_registration_row.dataset_path,
                    "target_cnt_of_zipfiles": 1,  # premium_synthetic_dataset의 경우는 매번 새롭게 1개의 상품만 만들것이므로 target은 1로 한다.
                    "current_cnt_of_zip_files": 0,  # premium_synthetic_dataset의 경우는 매번 새롭게 1개의 상품만 만들것이므로 current zip 파일은 0개로 한다.
                }

                if result_dict["product_type_id"] == 1 and \
                    sampling_row.product_registration_row.azoo_product[0].default_dataset_quantity != premium_dataset_quantity:
                    result_dict["product_type_id"] = 2
                    logger.info(f"product_type_id : 1 --> 2 로 변경 ")
                    pass

                return [
                    result_dict
                ]  # Premium Synthetic Dataset 은 패키징 작업에 item 이 1개인 list를 반환한다.

            elif params["from"] == "api_request_packaging()":
                product_registration_id = params["product_registration_id"]

                ############## 현재 packaged 수량 카운트
                sub_query = (
                    select(MSamplingStatusModel.id)
                    .where(func.lower(MSamplingStatusModel.name) == "Packaged".lower())
                    .subquery()
                )

                query = (
                    select(func.count())
                    .select_from(SamplingModel)
                    .where(
                        (
                            SamplingModel.product_registration_id
                            == product_registration_id
                        ),
                        (SamplingModel.is_deleted == False),
                        (SamplingModel.status_id == sub_query),
                    )
                )

                result = await db.execute(query)
                cnt_of_current_packaged = result.scalar()

                if cnt_of_current_packaged is None:
                    cnt_of_current_packaged = 0

                redis_client = RedisClient()
                already_packaging_queued_sampling_id_list = (
                    redis_client.get_package_process_list()
                )
                logger.info(
                    f"already_packaging_queued_sampling_id_list:{already_packaging_queued_sampling_id_list}"
                )

                sub_query = (
                    select(MSamplingStatusModel.id)
                    .where(func.lower(MSamplingStatusModel.name) == "Sampled".lower())
                    .subquery()
                )
                query = (
                    select(SamplingModel)
                    .options(
                        selectinload(SamplingModel.synthetic_task_row),
                        selectinload(
                            SamplingModel.product_registration_row
                        ).selectinload(ProductRegistrationModel.azoo_product),
                    )
                    .where(
                        (
                            SamplingModel.product_registration_id
                            == product_registration_id
                        ),
                        (SamplingModel.is_deleted == False),
                        (SamplingModel.status_id == sub_query),
                        (
                            SamplingModel.id.not_in(
                                already_packaging_queued_sampling_id_list
                            )
                        ),
                    )
                    .order_by(SamplingModel.created_at.asc())
                    .limit(10)
                )

                res = await db.execute(query)
                sampling_rows = res.fetchall()
                if len(sampling_rows) == 0:
                    logger.info(
                        f"현재 product_registration_id:{product_registration_id}에 대해, packaging 작업을 할 sampling rows가 존재하지 않습니다."
                    )
                    return []

                nums_of_buffer_zip = sampling_rows[0][
                    0
                ].product_registration_row.nums_of_buffer_zip
                target_elements_cnt = max(
                    nums_of_buffer_zip - cnt_of_current_packaged, 0
                )

                filtered_sampling_info_list = []
                for candidate_sampling_row in sampling_rows[:target_elements_cnt]:
                    try:
                        result_dict = {
                            "azoo_product_id": candidate_sampling_row[0]
                            .product_registration_row.azoo_product[0]
                            .id,
                            "product_type_id": candidate_sampling_row[
                                0
                            ].product_registration_row.product_type_id,  # 1:Common Dataset / 2:Premium Dataset / 3:Premium-Report
                            "sampling_table_row_id": candidate_sampling_row[0].id,
                            "product_registration_id": product_registration_id,
                            "random_sampled_list": candidate_sampling_row[
                                0
                            ].random_sampled_list,
                            "pkl_path": candidate_sampling_row[
                                0
                            ].synthetic_task_row.result_files_list_pkl_path,
                            "original_dataset_path": candidate_sampling_row[
                                0
                            ].product_registration_row.dataset_path,
                            "target_cnt_of_zipfiles": candidate_sampling_row[
                                0
                            ].product_registration_row.nums_of_buffer_zip,
                            "current_cnt_of_zip_files": cnt_of_current_packaged,
                        }


                        if result_dict["product_type_id"] == 1 and \
                            ( candidate_sampling_row[0].product_registration_row.azoo_product[0].default_dataset_quantity != candidate_sampling_row[0].len_sampled_list ):
                            result_dict["product_type_id"] = 2  # Common - Multiple Synthetic 의 경우는 Premium 취급 함
                            logger.info(f" sampling row id: {candidate_sampling_row[0].id} 는, Multiple Common Synthetic Dataset 이므로, Premium Synthetic 취급 함 --> product_type_id를 1에서 2로 변경")
                            pass


                        filtered_sampling_info_list.append(result_dict)
                        pass
                    except:
                        logger.error(traceback.format_exc())
                        continue
                    pass
                logger.info(
                    f"nums_of_buffer_zip:{nums_of_buffer_zip} , current_cnt_of_zip_files:{cnt_of_current_packaged} --> target_elements_cnt:{target_elements_cnt}"
                )
                logger.info(
                    f"len(sampling_rows):{len(sampling_rows)} , len(filtered_sampling_info_list):{len(filtered_sampling_info_list)}"
                )
                return filtered_sampling_info_list

            elif params["from"] == "api_random_sampling()":
                product_registration_row_id = params["product_registration_row_id"]
                task_id = params["synthetic_task_row_id"]

                sub_query = select(MSamplingStatusModel.id).where(
                    func.lower(MSamplingStatusModel.name) == "Sampled".lower()
                )
                query = (
                    select(func.count())
                    .select_from(SamplingModel)
                    .where(
                        (
                            SamplingModel.product_registration_id
                            == product_registration_row_id
                        )
                        & (SamplingModel.generaitng_synthetic_task_id == task_id)
                        & (SamplingModel.status_id == sub_query)
                    )
                )

                result = await db.execute(query)
                total_table_items = result.scalar()
                logger.info(
                    f"[Return] params:{params} , total_table_items:{total_table_items}"
                )
                return total_table_items

    except:
        logger.error(traceback.format_exc())
        raise
