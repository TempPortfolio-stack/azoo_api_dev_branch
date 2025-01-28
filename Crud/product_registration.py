import traceback
from datetime import datetime

from sqlalchemy.future import select

from Model.new_models import (
    ProductBaseInfoModel,
    GeneratingSyntheticTasksModel,
    ProductRegistrationModel,
    DatasetTypeModel,
    MProductTypeModel,
)
from Settings.Logger.logging_config import db_util_logger as logger


async def read(db, params: dict):
    try:
        logger.info(
            f"product_registration.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        async with db.begin():  ### 트랜젝션
            if params["from"] == "api_request_prepare_products()":
                product_registration_row_id = params["product_registration_row_id"]
                
                query = (
                    select(
                        ProductRegistrationModel.nums_of_buffer_sampling,
                        ProductRegistrationModel.nums_of_buffer_zip,
                        ProductRegistrationModel.nums_of_s3_product,
                        DatasetTypeModel.name,
                        MProductTypeModel.name,
                    )
                    .join(
                        ProductBaseInfoModel,
                        ProductRegistrationModel.product_base_info_id
                        == ProductBaseInfoModel.id,
                    )
                    .join(
                        DatasetTypeModel,
                        ProductBaseInfoModel.data_type_id == DatasetTypeModel.id,
                    )
                    .join(
                        MProductTypeModel,
                        ProductRegistrationModel.product_type_id
                        == MProductTypeModel.id,
                    )
                    .where((ProductRegistrationModel.id == product_registration_row_id))
                )
                

                result = await db.execute(query)
                row = result.first()
                (
                    nums_of_buffer_sampling,
                    nums_of_buffer_zip,
                    nums_of_s3_product,
                    data_type_name,
                    product_type_name,
                ) = row

                result_dict = {
                    "nums_of_buffer_sampling": nums_of_buffer_sampling,
                    "nums_of_buffer_zip": nums_of_buffer_zip,
                    "nums_of_s3_product": nums_of_s3_product,
                    "data_type_name": data_type_name,
                    "product_type_name": product_type_name,
                }
                logger.info(f"[Return] result_dict:{result_dict}")
                return (
                    nums_of_buffer_sampling,
                    nums_of_buffer_zip,
                    nums_of_s3_product,
                    data_type_name,
                    product_type_name,
                )
    except:
        logger.error(traceback.format_exc())
        raise


async def update(db, params: dict):
    try:
        logger.info(
            f"product_registration.py 의 update()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        if params["from"] == "make_dir_in_NAS_for_premium_product()":
            async with db.begin():
                target_dataset_id = params["product_registration_dataset_id"]

                query = select(ProductRegistrationModel).where(
                    ProductRegistrationModel.dataset_id == target_dataset_id
                )

                result = await db.execute(query)

                target_row = result.scalar()
                target_row.dataset_path = params["dataset_path"]
                target_row.updated_at = datetime.now()
                db.add(target_row)
                await db.commit()
                return None
        pass
    except:
        logger.error(traceback.format_exc())
        raise
