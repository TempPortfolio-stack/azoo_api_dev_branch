import traceback

from sqlalchemy import func
from sqlalchemy.future import select

from Model.new_models import (
    AzooProductModel,
    MAzooProductStatusModel,
)
from Settings.Logger.logging_config import db_util_logger as logger


async def read(db, params: dict):
    try:
        logger.info(
            f"azoo_product.py 의 read()에 작업 요청이 들어왔습니다. --> params:{params}"
        )
        async with db.begin():  ### 트랜젝션
            if params["from"] == "api_request_prepare_products()":
                azoo_product_id = params["azoo_product_id"]
                sub_query = select(MAzooProductStatusModel.id).where(
                    func.lower(MAzooProductStatusModel.name).in_(
                        ["Pending".lower(), "Active".lower()]
                    )
                )

                query = select(AzooProductModel.product_id , AzooProductModel.default_dataset_quantity).where(
                    (AzooProductModel.id == azoo_product_id)
                    & (AzooProductModel.is_deleted == False)
                    & (AzooProductModel.status_id.in_(sub_query))
                )

                res = await db.execute(query)
                product_registration_row_id , default_qty = res.fetchone()
                logger.info(
                    f"azoo_product 의 id:{azoo_product_id} 에 해당되는 product_registration_row_id:{product_registration_row_id} , default_qty:{default_qty}"
                )
                if product_registration_row_id is None:
                    raise Exception(
                        f"azoo_product id : {azoo_product_id} 에 적절한 product_registration id값이 할당되지 않았습니다."
                    )

                return product_registration_row_id , default_qty
    except:
        logger.error(traceback.format_exc())
        raise
