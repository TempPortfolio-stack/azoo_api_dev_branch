import traceback
from datetime import datetime

from sqlalchemy import func
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from Common.redis_token import RedisClient
from Model.new_models import (
    ProductWarehouseModel,
    MProductWarehouseStatusModel,
    SamplingModel,
    AzooProductModel,
    ProductRegistrationModel,
    PurchaseModel,
)
from Settings.Logger.logging_config import db_util_logger as logger


async def create(db, params: dict):
    try:
        logger.info(
            f"product_warehouse.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        if params["from"] == "api_insert_premium_synthetic_product_warehouse()":
            async with db.begin():  ### 트랜젝션
                subquery_sold_at = (
                    select(PurchaseModel.created_at)
                    .where(PurchaseModel.id == params["premium_purchase_id"])
                    .subquery()
                )

                new_row = ProductWarehouseModel()
                new_row.created_at = datetime.now()
                new_row.updated_at = datetime.now()
                new_row.is_deleted = False
                new_row.product_id = params["azoo_product_id"]
                new_row.purchase_id = params["premium_purchase_id"]
                new_row.sampling_id = params["sampling_table_row_id"]
                new_row.sold_at = subquery_sold_at
                sub_query = (
                    select(MProductWarehouseStatusModel.id)
                    .where(
                        func.lower(MProductWarehouseStatusModel.name)
                        == (params["set_status_to"].lower())
                    )
                    .subquery()
                )
                new_row.status_id = sub_query  # 5. Sold
                db.add(new_row)
                pass  # 트랜젝션 종료
            await db.refresh(new_row)
            return new_row.id

        elif params["from"] == "api_insert_premium_report_product_warehouse()":
            async with db.begin():  ### 트랜젝션
                subquery_sold_at = (
                    select(PurchaseModel.created_at)
                    .where(PurchaseModel.id == params["premium_purchase_id"])
                    .subquery()
                )

                new_row = ProductWarehouseModel()
                new_row.created_at = datetime.now()
                new_row.updated_at = datetime.now()
                new_row.is_deleted = False
                new_row.product_id = params["azoo_product_id"]
                new_row.purchase_id = params["premium_purchase_id"]
                new_row.sampling_id = params["sampling_table_row_id"]
                new_row.sold_at = subquery_sold_at
                sub_query = (
                    select(MProductWarehouseStatusModel.id)
                    .where(
                        func.lower(MProductWarehouseStatusModel.name)
                        == (params["set_status_to"].lower())
                    )
                    .subquery()
                )
                new_row.status_id = sub_query  # 5. Sold
                db.add(new_row)
                pass  # 트랜젝션 종료
            await db.refresh(new_row)
            return new_row.id

        pass
    except:
        logger.error(traceback.format_exc())
        raise
    pass


async def read(db, params: dict):
    try:
        logger.info(
            f"product_warehouse.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        async with db.begin():  ### 트랜젝션
            if (
                params["from"] == "api_request_uploading()"
            ):  # Common Synthetic Dataset 상품의 uploading

                result_dict = {}

                azoo_product_id = params["azoo_product_id"]
                sub_query = select(MProductWarehouseStatusModel.id).where(
                    func.lower(MProductWarehouseStatusModel.name).in_(
                        ["Uploading to S3".lower(), "Azoo Product".lower()]
                    )
                )

                query = (
                    select(func.count())
                    .select_from(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.product_id == azoo_product_id),
                        (ProductWarehouseModel.is_deleted == False),
                        (ProductWarehouseModel.status_id.in_(sub_query)),
                    )
                )

                query_result = await db.execute(query)
                cnt_of_current_azoo_product = query_result.scalar()

                if cnt_of_current_azoo_product is None:
                    cnt_of_current_azoo_product = 0

                redis_client = RedisClient()
                already_uploading_queued_product_warehouse_id_list = (
                    redis_client.get_s3_uploading_process_list()
                )
                logger.info(
                    f"already_uploading_product_warehouse_id_list:{already_uploading_queued_product_warehouse_id_list}"
                )

                query = (
                    select(ProductWarehouseModel)
                    .join(
                        MProductWarehouseStatusModel,
                        ProductWarehouseModel.status_id
                        == MProductWarehouseStatusModel.id,
                    )
                    .options(
                        selectinload(ProductWarehouseModel.sampling_row)
                        .selectinload(SamplingModel.product_registration_row)
                        .selectinload(ProductRegistrationModel.product_base_info_row)
                    )
                    .where(
                        (
                            func.lower(MProductWarehouseStatusModel.name)
                            == "Pending".lower()
                        ),
                        (ProductWarehouseModel.product_id == azoo_product_id),
                        (
                            ProductWarehouseModel.id.not_in(
                                already_uploading_queued_product_warehouse_id_list
                            )
                        ),
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                    .limit(10)
                )

                query_result = await db.execute(query)
                product_warehouse_rows = query_result.fetchall()

                nums_of_s3_product = 20  # 20개 정도면 충분
                target_elements_cnt = max(
                    nums_of_s3_product - cnt_of_current_azoo_product, 0
                )

                logger.info(
                    f" nums_of_s3_product:{nums_of_s3_product} , cnt_of_current_azoo_product:{cnt_of_current_azoo_product} , target_elements_cnt = (max( nums_of_s3_product - cnt_of_current_azoo_product , 0))  ---> target_elements_cnt:{target_elements_cnt}"
                )
                filtered_product_warehouse_list = []
                for candidate_upload_product_row in product_warehouse_rows[
                    :target_elements_cnt
                ]:
                    try:
                        if candidate_upload_product_row[0].sampling_row is None:
                            # ProductWarehouse 테이블에서 sampling_row_id 가 None == synthetic dataset 상품이 아니다.
                            continue

                        result_dict = {
                            "azoo_product_id": azoo_product_id,
                            "sampling_id": candidate_upload_product_row[
                                0
                            ].sampling_row.id,
                            "mnt_path_in_nas": candidate_upload_product_row[
                                0
                            ].sampling_row.mnt_path_in_nas,
                            "dataset_id": str(
                                candidate_upload_product_row[
                                    0
                                ].sampling_row.product_registration_row.dataset_id
                            ),
                            "product_warehouse_id": candidate_upload_product_row[0].id,
                            "nums_of_s3_product": candidate_upload_product_row[
                                0
                            ].sampling_row.product_registration_row.nums_of_s3_product,
                            "cnt_of_current_azoo_product": cnt_of_current_azoo_product,
                            "product_type_id": candidate_upload_product_row[
                                0
                            ].sampling_row.product_registration_row.product_type_id,  # 1. Common Dataset / 2. Premium Dataset / 3. Premium-Report
                            "data_type_id": candidate_upload_product_row[
                                0
                            ].sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                        }
                        filtered_product_warehouse_list.append(result_dict)
                        pass
                    except:
                        logger.error(traceback.format_exc())
                        continue
                    pass

                logger.info(
                    f"nums_of_s3_product:{nums_of_s3_product} , cnt_of_current_azoo_product:{cnt_of_current_azoo_product} --> target_elements_cnt:{target_elements_cnt}"
                )
                logger.info(
                    f"len(product_warehouse_rows):{len(product_warehouse_rows)} , len(filtered_product_warehouse_list):{len(filtered_product_warehouse_list)}"
                )
                return filtered_product_warehouse_list

            elif (
                params["from"] == "api_request_premium_synthetic_uploading()"
            ):  # Premium Synthetic Dataset 상품의 uploading
                # 이걸 만들어 내야해
                azoo_product_id = params["azoo_product_id"]
                premium_purchase_id = params["premium_purchase_id"]
                sampling_table_id = params["sampling_table_id"]

                query = (
                    select(ProductWarehouseModel, SamplingModel)
                    .join(
                        SamplingModel,
                        ProductWarehouseModel.sampling_id == SamplingModel.id,
                    )
                    .options(
                        selectinload(
                            SamplingModel.product_registration_row
                        ).selectinload(ProductRegistrationModel.product_base_info_row),
                        selectinload(SamplingModel.product_warehouse_row).selectinload(
                            ProductWarehouseModel.purchase_row
                        ),
                    )
                    .where(
                        (
                            func.lower(MProductWarehouseStatusModel.name)
                            == "Pending".lower()
                        ),
                        (
                            ProductWarehouseModel.purchase_id == premium_purchase_id
                        ),  # purchase_id 를 이용해서, 판매된 상품들의 정보를 얻고,
                        (
                            ProductWarehouseModel.product_id == azoo_product_id
                        ),  # azoo_product_id 를 통해서, 판매된 상품이 Premium_Dataset 인지, Premium_Report 인지 구분할 수 있다.
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                    .limit(1)
                )

                result = await db.execute(query)
                product_warehouse_row, sampling_row = result.first()

                req_info_dict = {
                    "azoo_product_id": azoo_product_id,
                    "sampling_id": sampling_table_id,
                    "current_purchase_status_id": sampling_row.product_warehouse_row[
                        0
                    ].purchase_row.status_id,
                    "mnt_path_in_nas": sampling_row.mnt_path_in_nas,  # Premium_synthetic 의 경우에는.. 랜덤샘플링 , 패키징을 처리하므로.. sampling table에 남는다.
                    "dataset_id": sampling_row.product_registration_row.dataset_id,
                    "product_warehouse_id": product_warehouse_row.id,
                    "product_type_id": 2,  # 2. Premium Synthetic Dataset  ,  3. Premium-Report
                    "data_type_id": sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    "premium_purchase_id": premium_purchase_id,
                }
                return req_info_dict

            elif (
                params["from"] == "api_request_premium_report_uploading()"
            ):  # Premium Report 상품의 uploading
                # 이걸 만들어 내야해
                azoo_product_id = params["azoo_product_id"]
                premium_purchase_id = params["premium_purchase_id"]
                sampling_table_id = params["sampling_table_id"]

                query = (
                    select(ProductWarehouseModel, SamplingModel)
                    .join(
                        SamplingModel,
                        ProductWarehouseModel.sampling_id == SamplingModel.id,
                    )
                    .options(
                        selectinload(
                            SamplingModel.product_registration_row
                        ).selectinload(ProductRegistrationModel.product_base_info_row),
                        selectinload(SamplingModel.product_warehouse_row).selectinload(
                            ProductWarehouseModel.purchase_row
                        ),
                    )
                    .where(
                        (
                            func.lower(MProductWarehouseStatusModel.name)
                            == "Pending".lower()
                        ),
                        (
                            ProductWarehouseModel.purchase_id == premium_purchase_id
                        ),  # purchase_id 를 이용해서, 판매된 상품들의 정보를 얻고,
                        (
                            ProductWarehouseModel.product_id == azoo_product_id
                        ),  # azoo_product_id 를 통해서, 판매된 상품이 Premium_Dataset 인지, Premium_Report 인지 구분할 수 있다.
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                    .limit(1)
                )

                result = await db.execute(query)
                product_warehouse_row, sampling_row = result.first()

                req_info_dict = {
                    "azoo_product_id": azoo_product_id,
                    "sampling_id": sampling_table_id,
                    "current_purchase_status_id": sampling_row.product_warehouse_row[
                        0
                    ].purchase_row.status_id,
                    "mnt_path_in_nas": sampling_row.mnt_path_in_nas,  # Premium_synthetic 의 경우에는.. 랜덤샘플링 , 패키징을 처리하므로.. sampling table에 남는다.
                    "dataset_id": sampling_row.product_registration_row.dataset_id,
                    "product_warehouse_id": product_warehouse_row.id,
                    "product_type_id": 3,  # 2. Premium Synthetic Dataset  ,  3. Premium-Report
                    "data_type_id": sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    "premium_purchase_id": premium_purchase_id,
                }
                return req_info_dict

            elif (
                params["from"] == "api_request_original_product_uploading()"
            ):  # Original Dataset 상품의 uploading
                azoo_product_id = params["azoo_product_id"]

                sub_query = (
                    select(MProductWarehouseStatusModel.id)
                    .where(
                        func.lower(MProductWarehouseStatusModel.name).in_(
                            ["Uploading to S3".lower(), "Azoo Product".lower()]
                        )
                    )
                    .subquery()
                )

                # filter(GeneratingSyntheticTasksModel.task_status_id.in_(status_ids) & GeneratingSyntheticTasksModel.is_deleted.is_(False) & GeneratingSyntheticTasksModel.task_id.is_not(None))
                query = (
                    select(func.count())
                    .select_from(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.product_id == azoo_product_id)
                        & (ProductWarehouseModel.is_deleted == False)
                        & (ProductWarehouseModel.status_id.in_(sub_query))
                    )
                )
                res = await db.execute(query)
                cnt_of_current_azoo_product = res.scalar()
                if cnt_of_current_azoo_product is None:
                    cnt_of_current_azoo_product = 0

                sub_query = (
                    select(MProductWarehouseStatusModel.id)
                    .where(
                        func.lower(MProductWarehouseStatusModel.name)
                        == "Pending".lower()
                    )
                    .subquery()
                )

                query = (
                    select(ProductWarehouseModel)
                    .options(
                        selectinload(ProductWarehouseModel.azoo_product)
                        .selectinload(AzooProductModel.product_registration_row)
                        .selectinload(ProductRegistrationModel.product_base_info_row)
                    )
                    .where(
                        (ProductWarehouseModel.product_id == azoo_product_id)
                        & (ProductWarehouseModel.is_deleted == False)
                        & (ProductWarehouseModel.status_id == sub_query)
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                    .limit(10)
                )

                res = await db.execute(query)
                product_warehouse_rows = res.scalars().all()
                nums_of_s3_product = product_warehouse_rows[
                    0
                ].azoo_product.product_registration_row.nums_of_s3_product
                target_elements_cnt = max(
                    nums_of_s3_product - cnt_of_current_azoo_product, 0
                )
                logger.info(
                    f" nums_of_s3_product:{nums_of_s3_product} , cnt_of_current_azoo_product:{cnt_of_current_azoo_product} , target_elements_cnt = (max( nums_of_s3_product - cnt_of_current_azoo_product , 0))  ---> target_elements_cnt:{target_elements_cnt}"
                )
                logger.info(f"target_elements_cnt:{target_elements_cnt}")

                filtered_product_warehouse_list = []
                for candidate_upload_product_row in product_warehouse_rows[
                    :target_elements_cnt
                ]:
                    try:
                        product_warehouse_id = candidate_upload_product_row.id
                        product_registration_row = (
                            candidate_upload_product_row.azoo_product.product_registration_row
                        )
                        nums_of_s3_product = product_registration_row.nums_of_s3_product
                        dataset_id = str(product_registration_row.dataset_id)
                        mnt_path_in_nas = f"{product_registration_row.dataset_path}/zip_original_dataset/{product_registration_row.dataset_id}.zip"
                        nums_of_buffer_zip = product_registration_row.nums_of_buffer_zip

                        result_dict = {
                            "dataset_id": dataset_id,
                            "product_warehouse_id": product_warehouse_id,
                            "azoo_product_id": azoo_product_id,
                            "mnt_path_in_nas": mnt_path_in_nas,
                            "nums_of_s3_product": nums_of_s3_product,
                            "cnt_of_current_azoo_product": cnt_of_current_azoo_product,
                            "nums_of_buffer_zip": nums_of_buffer_zip,
                            "product_type_id": candidate_upload_product_row.azoo_product.product_registration_row.product_type_id,  # 1. Common Dataset / 2. Premium Dataset / 3. Premium-Report
                            "data_type_id": candidate_upload_product_row.azoo_product.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                        }
                        filtered_product_warehouse_list.append(result_dict)
                    except:
                        logger.error(traceback.format_exc())
                        continue
                    pass

                logger.info(
                    f"[Return] filtered_product_warehouse_list:{filtered_product_warehouse_list}"
                )
                return filtered_product_warehouse_list

    except:
        logger.error(traceback.format_exc())
        raise
