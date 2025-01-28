import traceback
from datetime import datetime, timedelta

from sqlalchemy import update, func
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

# from Model.models import (ProductRegistrationModel , UploadStatusModel , ContractModel , MContractStatusModel , MProductWarehouseStatusModel , AzooProductModel , MAzooProductStatusModel , MPurchaseStatusModel ,
#                           GeneratingSyntheticTasksModel , MgeneratingSyntheticTaskStatus , MSamplingStatusModel , SamplingModel , ProductWarehouseModel, PurchaseModel , UserModel)
from Model.new_models import (
    ProductRegistrationModel,
    UploadStatusModel,
    ContractModel,
    MContractStatusModel,
    MProductWarehouseStatusModel,
    AzooProductModel,
    MAzooProductStatusModel,
    MPurchaseStatusModel,
    GeneratingSyntheticTasksModel,
    MgeneratingSyntheticTaskStatus,
    MSamplingStatusModel,
    SamplingModel,
    ProductWarehouseModel,
    PurchaseModel,
    UserModel,
    ProductBaseInfoModel,
    OrderModel,
)
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import db_util_logger as logger


# 018f7542-f329-736a-a6c1-b24a8d25dbdc --> id:35


async def get_expired_products_list(db=None):  # Done
    try:
        logger.info("get_expired_products_list() 에서 작업 시작합니다. >>>>>>")
        async with db.begin():  ### 트랜젝션

            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            sub_query = (
                select(MProductWarehouseStatusModel.id)
                .where(func.lower(MProductWarehouseStatusModel.name) == "Sold".lower())
                .subquery()
            )

            query = (
                select(ProductWarehouseModel.id, ProductWarehouseModel.s3_key)
                .where(
                    (
                        ProductWarehouseModel.purchase_id.is_not(None)
                    )  # 구매이력 있는 상품.
                    & (
                        ProductWarehouseModel.sampling_id.is_not(None)
                    )  # synthetic data 만, 왜냐하면, original_dataset 은 1개 zip파일 올려놓고 계속 쓰고있기 때문.
                    & (
                        ProductWarehouseModel.expired_date < datetime.now()
                    )  # 다운로드 만료기간 지난 파일들..
                    & (ProductWarehouseModel.status_id == sub_query)
                )
                .order_by(ProductWarehouseModel.created_at.asc())
                .limit(30)
            )

            res = await db.execute(query)
            row_ids = res.fetchall()
            logger.info(
                f"현재시점 기준: {datetime.now()} // expired_date 가 지난, product_warehouse 의 ids 는 다음과 같습니다--> {row_ids}"
            )
            return row_ids
        pass
    except:
        logger.error(f"get_expired_products_list() 를 수행하지 못했습니다.")
        logger.error(traceback.format_exc())
        raise
    pass


async def update_product_table_status(
    db=None, target_dataset_id="", status_to=""
):  # Done
    try:
        logger.info(
            f"update_product_table_status() 에 요청이 들어왔습니다. >>>>>>> target_dataset_id:{target_dataset_id} , status_to:{status_to}"
        )
        async with db.begin():
            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            status_sub_query = (
                select(UploadStatusModel.id)
                .where(func.lower(UploadStatusModel.name) == status_to.lower())
                .subquery()
            )

            # 현재 upload_status_id 값을 가져옵니다.
            base_info_sub_query = (
                select(ProductBaseInfoModel)
                .join(
                    ProductRegistrationModel,
                    ProductRegistrationModel.product_base_info_id
                    == ProductBaseInfoModel.id,
                )
                .where(ProductRegistrationModel.dataset_id == target_dataset_id)
                .subquery()
            )

            # 상태값 업데이트를 수행합니다.
            if status_to.lower() == "zipped":  ### 정상적으로 zip이 완료되었다.
                await db.execute(
                    update(ProductRegistrationModel)
                    .where(
                        (ProductRegistrationModel.dataset_id == target_dataset_id),
                        (base_info_sub_query.c.upload_status_id < status_sub_query),
                    )
                    .values(
                        dataset_path=f"/mnt/nfs/azoo/{settings['DEPLOY_MODE']}/{target_dataset_id}",
                        updated_at=datetime.now(),
                    )
                )
                logger.info(
                    f"ProductRegistrationModel의 dataset_path 를 update 합니다. >>>>>>> /mnt/nfs/azoo/{settings['DEPLOY_MODE']}/{target_dataset_id} "
                )

                await db.execute(
                    update(ProductBaseInfoModel)
                    .where(
                        (ProductBaseInfoModel.id == base_info_sub_query.c.id),
                        (base_info_sub_query.c.upload_status_id < status_sub_query),
                    )
                    .values(
                        upload_status_id=status_sub_query, updated_at=datetime.now()
                    )
                )
                logger.info(
                    f"ProductBaseInfoModel upload_status_id 를 update 합니다. >>>>>>> status_to:{status_to} "
                )
            else:
                await db.execute(
                    update(ProductBaseInfoModel)
                    .where(
                        (ProductBaseInfoModel.id == base_info_sub_query.c.id),
                        (
                            (base_info_sub_query.c.upload_status_id < status_sub_query)
                            | (base_info_sub_query.c.upload_status_id.is_(None))
                        ),
                    )
                    .values(
                        upload_status_id=status_sub_query, updated_at=datetime.now()
                    )
                )
                logger.info(
                    f"ProductBaseInfoModel upload_status_id 를 update 합니다. >>>>>>> status_to:{status_to} "
                )
            await db.commit()
            logger.info(
                f"{target_dataset_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"dataset_id: {target_dataset_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_contract_table_status(
    db=None, contract_unique_id="", status_to=""
):  # Done
    try:
        logger.info(
            f"update_contract_table_status() 에 요청이 들어왔습니다. >>>>>>>> contract_unique_id:{contract_unique_id} , status_to:{status_to}"
        )
        async with db.begin():
            sub_query = (
                select(MContractStatusModel.id)
                .where(func.lower(MContractStatusModel.name) == status_to.lower())
                .subquery()
            )

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(ContractModel)
                .where(
                    (ContractModel.contract_unique_id == contract_unique_id),
                    (
                        (ContractModel.contract_status_id < sub_query)
                        | (ContractModel.contract_status_id.is_(None))
                    ),
                )
                .values(contract_status_id=sub_query, updated_at=datetime.now())
            )
            await db.commit()
            logger.info(
                f"{contract_unique_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"dataset_id: {contract_unique_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_synthetic_task_table_status(
    db=None, row_id=0, status_to="", result_files_list_pkl_path="", cnt_rows_of_pkl=0
):  # Done
    try:
        logger.info(
            f"update_synthetic_task_table_status() 에 요청이 들어왔습니다. >>>>> row_id:{row_id} , status_to:{status_to} , result_files_list_pkl_path:{result_files_list_pkl_path} , cnt_rows_of_pkl:{cnt_rows_of_pkl} "
        )
        async with db.begin():
            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            sub_query = select(MgeneratingSyntheticTaskStatus.id).where(
                func.lower(MgeneratingSyntheticTaskStatus.name) == status_to.lower()
            )

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(GeneratingSyntheticTasksModel)
                .where(
                    (GeneratingSyntheticTasksModel.id == row_id),
                    (
                        (GeneratingSyntheticTasksModel.task_status_id <= sub_query)
                        | (GeneratingSyntheticTasksModel.task_status_id.is_(None))
                    ),
                )
                .values(
                    task_status_id=sub_query,
                    result_files_list_pkl_path=result_files_list_pkl_path,
                    cnt_rows_of_pkl=cnt_rows_of_pkl,
                    updated_at=datetime.now(),
                )
            )
            await db.commit()
            logger.info(
                f"row_id: {row_id} 의 상태를 {status_to} 로 성공적으로 업데이트하였습니다."
            )
            return
    except:
        logger.error(f"row_id: {row_id} 의 상태값을 {status_to}로 변경하지 못했습니다.")
        logger.error(traceback.format_exc())
        return


async def insert_sampled_list_to_table(
    db=None, sampled_list=[], product_registration_row_id=0, synthetic_task_id=""
):  # Done
    try:
        logger.info(
            f"insert_sampled_list_to_table() 에 요청이 들어왔습니다. >>>>>> len(sampled_list):{ None if sampled_list is None else len(sampled_list) } , product_registration_row_id:{product_registration_row_id} , synthetic_task_id:{synthetic_task_id}"
        )
        async with db.begin():
            # Step 1: task_id --> task_id's row_id
            subquery_synthetic_task_row_id = (
                select(GeneratingSyntheticTasksModel.id)
                .where(GeneratingSyntheticTasksModel.task_id == synthetic_task_id)
                .subquery()
            )

            # Step 2: 'Sampled'인 MSamplingStatusModel의 id 값을 조회
            subquery_sampling_status_id = (
                select(MSamplingStatusModel.id)
                .where(func.lower(MSamplingStatusModel.name) == "Sampled".lower())
                .subquery()
            )

            # Step 3: SamplingModel에 데이터 삽입
            new_sampling = SamplingModel(
                created_at=datetime.now(),
                updated_at=datetime.now(),
                is_deleted=False,
                product_registration_id=product_registration_row_id,
                generaitng_synthetic_task_id=subquery_synthetic_task_row_id,
                status_id=subquery_sampling_status_id,
                random_sampled_list=str(sampled_list),
                len_sampled_list=len(sampled_list),
            )
            db.add(new_sampling)
            await db.commit()
        logger.info(f"SamplingModel에 row_id: {new_sampling.id} 가 insert 되었습니다.")
        return new_sampling.id
    except:
        logger.error(
            f"product_registration_id: {product_registration_row_id} , synthetic_task_id:{synthetic_task_id} , {sampled_list} 가 insert 되지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_sampling_table_status(
    db=None, row_id=0, status_to="", mnt_path_in_nas=""
):  # Done
    try:
        logger.info(
            f"update_sampling_table_status() 에 요청이 들어왔습니다. >>>>>>> row_id:{row_id} , status_to:{status_to} , mnt_path_in_nas:{mnt_path_in_nas}"
        )
        async with db.begin():
            subquery_status_to = (
                select(MSamplingStatusModel.id)
                .where(func.lower(MSamplingStatusModel.name) == status_to.lower())
                .subquery()
            )
            # subquery_current_status_id = select(SamplingModel.status_id).where( SamplingModel.id == row_id).subquery()

            # 상태값 업데이트를 수행합니다.
            if mnt_path_in_nas is not None:  ##
                await db.execute(
                    update(SamplingModel)
                    .where(
                        (SamplingModel.id == row_id),
                        (
                            (SamplingModel.status_id <= subquery_status_to)
                            | (SamplingModel.status_id.is_(None))
                        ),
                    )
                    .values(
                        status_id=subquery_status_to,
                        updated_at=datetime.now(),
                        mnt_path_in_nas=mnt_path_in_nas,
                    )
                )
            else:
                await db.execute(
                    update(SamplingModel)
                    .where(
                        (SamplingModel.id == row_id),
                        (
                            (SamplingModel.status_id <= subquery_status_to)
                            | (SamplingModel.status_id.is_(None))
                        ),
                    )
                    .values(status_id=subquery_status_to, updated_at=datetime.now())
                )
            pass

            await db.commit()
            logger.info(
                f"SamplingModel 의 row_id: {row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(f"row_id: {row_id} 의 상태값을 {status_to}로 변경하지 못했습니다.")
        logger.error(traceback.format_exc())
        return


async def insert_product_warehouse_table(db=None, azoo_product_id=0, status=""):  # Done
    try:
        logger.info(
            f"insert_product_warehouse_table() 에 요청이 들어왔습니다.  >>>>>>>>  azoo_product_id:{azoo_product_id} , status:{status}"
        )
        #####################
        # 1. data.azoo_product_id.product_registration_id 를 가져옴
        #
        # 2. sampling 테이블에서, data.azoo_product_id.product_registration_id 에 해당되는 , status 가 packaged 인 id 를 가져옴
        #
        # 3.
        ####################
        async with db.begin():
            # Step 1: AzooProduct id 에 해당되는 product_registration 테이블의 row_id 가져옴
            query = (
                select(AzooProductModel)
                .options(selectinload(AzooProductModel.product_registration_row))
                .where((AzooProductModel.id == azoo_product_id))
            )
            result = await db.execute(query)
            row = result.scalar()
            if row is None:
                raise Exception(
                    f"{azoo_product_id} 값은 현재 azoo_product 테이블에 존재하지 않습니다."
                )

            product_id = row.product_id
            target_nums_of_buffer_zip = row.product_registration_row.nums_of_buffer_zip
            logger.info(
                f"azoo_product_id:{azoo_product_id} --->  product_registration_id:{product_id} , target_nums_of_buffer_zip:{target_nums_of_buffer_zip}"
            )

            # Step 2: sampling 테이블에서, product_id 에 해당되는 status가 packaged 인 id를 가져옴. ( Common : Ok , Premium : Ok , Report : No!! )
            subquery_status_id = (
                select(MSamplingStatusModel.id)
                .where(func.lower(MSamplingStatusModel.name) == "Packaged".lower())
                .subquery()
            )

            # Step 3: product_warehouse 테이블에 이미 등록되어 있는 sampling_id 들을 가져온다.
            subquery_already_registered_list = (
                select(ProductWarehouseModel.sampling_id)
                .where(ProductWarehouseModel.product_id == azoo_product_id)
                .subquery()
            )

            # Step 4: Sampling 테이블에서, packaged 상태인 rows 중에서, 이미 product_warehouse 에 등록된 것은 제한 rows 중에서,  target_nums_of_buffer_zip 만큼만 가져온다.
            result = await db.execute(
                select(SamplingModel.id)
                .where(
                    (SamplingModel.status_id == subquery_status_id)
                    & (SamplingModel.product_registration_id == product_id)
                    & (SamplingModel.id.not_in(subquery_already_registered_list))
                )
                .order_by(SamplingModel.created_at.asc())
                .limit(target_nums_of_buffer_zip)
            )

            row_id_list = result.scalars().all()
            logger.info(f"SamplingModel 에서 선택된 row_ids : {row_id_list}")

            # Step 5: ProductWareHouse 에 Insert 하기 위해서, Pending 상태의 status_id 값을 가져온다.
            subquery_status_to = (
                select(MProductWarehouseStatusModel.id)
                .where(func.lower(MProductWarehouseStatusModel.name) == status.lower())
                .subquery()
            )

            # Step 6: ProductWareHouse 에 데이터 삽입
            for row_id in row_id_list:
                new_warehouse_product = ProductWarehouseModel(
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    is_deleted=False,
                    sampling_id=row_id,
                    product_id=azoo_product_id,
                    status_id=subquery_status_to,
                )
                db.add(new_warehouse_product)
                # await db.refresh(new_warehouse_product)  # row 새로고침
                # logger.info(f"row_id: {new_warehouse_product.id} 가 insert 되었습니다.")
                pass
            await db.commit()
            logger.info(f"azoo_product_id: {azoo_product_id} 가 insert 되었습니다.")
            return
        pass
    except:
        logger.error(f"azoo_product_id: {azoo_product_id} 가 insert 되지 못했습니다.")
        logger.error(traceback.format_exc())
        return


async def insert_product_warehouse_table_of_original_dataset(
    db=None, azoo_product_id=0, status=""
):  # Done
    try:
        # insert_product_warehouse_table_of_original_dataset(db=db , azoo_product_id=data.azoo_product_id , status = "Pending")
        #####################
        # 1. data.azoo_product_id.product_registration_id 를 가져옴
        #
        # 2. original dataset 형태로 판매되는 상품의 정보를 product_warehouse 테이블에 insert 함.
        ####################
        logger.info(
            f"insert_product_warehouse_table_of_original_dataset() 에 요청이 들어왔습니다. >>>>>> azoo_product_id:{azoo_product_id} , status:{status}"
        )
        async with db.begin():
            # Step 1: AzooProduct id 에 해당되는 product_registration 테이블의 row_id 가져옴
            query = (
                select(AzooProductModel, ProductRegistrationModel, ProductBaseInfoModel)
                .join(
                    ProductRegistrationModel,
                    AzooProductModel.product_id == ProductRegistrationModel.id,
                )
                .join(
                    ProductBaseInfoModel,
                    ProductRegistrationModel.product_base_info_id
                    == ProductBaseInfoModel.id,
                )
                .options(selectinload(ProductBaseInfoModel.upload_status))
                .where((AzooProductModel.id == azoo_product_id))
            )

            result = await db.execute(query)
            row = result.first()
            if row is None:
                raise Exception(
                    f"{azoo_product_id} 값은 현재 azoo_product 테이블에 존재하지 않습니다."
                )

            azoo_product = row[0]
            product_registration = row[1]
            product_base_info = row[2]

            product_registration_row_id = product_registration.id
            target_nums_of_buffer_zip = product_registration.nums_of_buffer_zip

            logger.info(
                f"azoo_product_id:{azoo_product_id} --->  product_registraion_id:{product_registration_row_id} --> product_base_info_id:{product_base_info.id} , {product_base_info.upload_status.name}"
            )
            if "zipped" not in product_base_info.upload_status.name.lower():
                logger.error(
                    f"azoo_product_id:{azoo_product_id} --->  product_registration_id:{product_registration_row_id} 는 zip파일이 준비되지 않았습니다."
                )
                raise Exception(
                    f"azoo_product_id: {azoo_product_id} 의 original dataset 의 zip파일이 준비되지 않았습니다."
                )

            # Step 2: ProductWareHouse 에 Insert 하기 위해서, Pending 상태의 status_id 값을 가져온다.
            subquery_status_id = (
                select(MProductWarehouseStatusModel.id)
                .where(func.lower(MProductWarehouseStatusModel.name) == status.lower())
                .subquery()
            )

            # Step 3: ProductWareHouse 에 데이터 삽입
            for _ in range(target_nums_of_buffer_zip):
                new_warehouse_product = ProductWarehouseModel(
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    is_deleted=False,
                    sampling_id=None,
                    product_id=azoo_product_id,
                    status_id=subquery_status_id,
                )
                db.add(new_warehouse_product)
                pass
            await db.commit()
            logger.info(
                f"ProductWareHouse 에 azoo_product_id:{azoo_product_id} 의 상품 --> {target_nums_of_buffer_zip}개 insert 되었습니다."
            )
            return
        pass
    except:
        logger.error(
            f"insert_product_warehouse_table_of_original_dataset 작업 실패했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def get_product_warehouse_row_id(db=None, purchase_id=0, because_of=""):  # Done
    try:
        logger.info(
            f"get_product_warehouse_row_id() 에 요청이 들어왔습니다. >>>>>> purchase_id:{purchase_id} , because_of:{because_of}"
        )
        async with db.begin():

            if (
                because_of == "noti_purchased"
            ):  # 일반 Common Product ( Common Dataset + Common Report) 가 판매 되었을 때, 호출 됨,
                # Common Report 는 따로 azoo_product에 없으므로.. Common Dataset 에 대한 azoo_product 와 매핑시킨 정보만 가져오면 됨

                # query = select(PurchaseModel.order_row.dataset_product_id, PurchaseModel.created_at)\
                #         .options(
                #             selectinload(PurchaseModel.order_row)
                #         )\
                #         .where(PurchaseModel.id == purchase_id)

                query = (
                    select(OrderModel.dataset_product_id, PurchaseModel.created_at)
                    .join(PurchaseModel, PurchaseModel.order_id == OrderModel.id)
                    .where(PurchaseModel.id == purchase_id)
                )

                result = await db.execute(query)
                row = result.first()
                if row is None:
                    raise Exception(
                        f"purchase_id: {purchase_id}에 해당되는 정보가 존재하지 않습니다. ---> 상품을 준비해야 합니다."
                    )

                azoo_product_id, sold_at = row
                logger.info(
                    f"azoo_product_id , sold_at : {azoo_product_id} , {sold_at}"
                )
                subquery_status_id = (
                    select(MProductWarehouseStatusModel.id)
                    .where(
                        func.lower(MProductWarehouseStatusModel.name)
                        == "Azoo Product".lower()
                    )
                    .subquery()
                )
                query = (
                    select(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.status_id == subquery_status_id),
                        (ProductWarehouseModel.purchase_id.is_(None)),
                        (ProductWarehouseModel.product_id == azoo_product_id),
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                )

                query_result = await db.execute(query)
                result = query_result.scalar()
                product_warehouse = result
                product_warehouse_row_id = product_warehouse.id

                logger.info(
                    f"ProductWarehouse 테이블에서 purchase_id:{purchase_id} 와 매칭되는 noti_purchased() --> product_warehouse_row_id: {product_warehouse_row_id} / sold_at:{sold_at} / azoo_product_id:{azoo_product_id} 리턴합니다."
                )
                return sold_at, product_warehouse_row_id, azoo_product_id

            elif (
                because_of == "noti_canceled"
            ):  # Common / Premium Product 가 cancel 되었을 때, 호출 됨
                ### ProductWarehouse 테이블에서 , purchase_id를 이용해서 해당되는 rows 들을 가져옴.
                ##### 상태값이 Azoo Product 이면서, product_id 가 일치하는 row 가져오기
                query = (
                    select(ProductWarehouseModel.id)
                    .where(ProductWarehouseModel.purchase_id == purchase_id)
                    .order_by(ProductWarehouseModel.created_at.asc())
                )

                query_result = await db.execute(query)
                product_warehouse_row_ids = query_result.scalars().all()

                if product_warehouse_row_ids is None:
                    raise Exception(
                        f"purchase_id: {purchase_id}에 해당되는 판매 기록이 product_warehouse 테이블에 존재하지 않습니다."
                    )

                logger.info(
                    f"ProductWarehouse 테이블에서 purchase_id 와 매칭되는 noti_canceled() --> product_warehouse_row_ids: {product_warehouse_row_ids} 리턴합니다."
                )
                return product_warehouse_row_ids
    except:
        logger.error(
            f"purchase_id: {purchase_id} 에 해당되는 product 를 매칭하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        raise


async def get_dataset_id_by_purchase_id(db=None, purchase_id=None):  # Done
    try:
        logger.info(
            f"get_dataset_id_by_purchase_id 에 요청이 들어왔습니다. >>>>>> purchase_id:{purchase_id}"
        )
        async with db.begin():
            # Purchase 테이블에서, order_id 를 가져온다.
            # order_id 를 이용해서, Order 테이블에서 dataset_product_id , dataset_product_quantity , report_product_id , report_product_quantity
            # azoo_product 테이블에서, dataset_product_id 와 report_product_id 를 이용해서, dataset_id 를 가져온다.

            query = (
                select(OrderModel, PurchaseModel)
                .join(PurchaseModel, PurchaseModel.order_id == OrderModel.id)
                .options(
                    selectinload(OrderModel.dataset_azoo_product).selectinload(
                        AzooProductModel.product_registration_row
                    ),
                    selectinload(OrderModel.report_azoo_product).selectinload(
                        AzooProductModel.product_registration_row
                    ),
                )
                .where(PurchaseModel.id == purchase_id)
            )

            query_result = await db.execute(query)
            order_row, purchase_row = query_result.first()  # 첫 번째 결과 행 가져오기

            dataset_id_dict = {}

            if order_row.dataset_azoo_product is None:
                raise Exception(
                    f"purchase_id : {purchase_id} , order_row_id :{order_row.id} 의 dataset_azoo_product 가 None 입니다. ---> Raise"
                )

            dataset_product_dataset_id = (
                order_row.dataset_azoo_product.product_registration_row.dataset_id
            )
            dataset_product_quantity = order_row.dataset_product_quantity

            if (
                order_row.dataset_azoo_product.product_registration_row.product_type_id
                == 1
            ):  # Common Dataset(Common Synthetic Dataset)
                if order_row.dataset_azoo_product.default_dataset_quantity == dataset_product_quantity:
                    dataset_id_dict["common_dataset_product"] = (
                        str(dataset_product_dataset_id),
                        dataset_product_quantity,
                    )                    
                else:
                    dataset_id_dict["premium_dataset_product"] = (
                    str(dataset_product_dataset_id),
                    dataset_product_quantity,
                    )

                logger.info(
                        f"purchase_id:{purchase_id} 에 대응되는 ---> order 테이블의 COMMON-dataset_product_dataset_id:{dataset_product_dataset_id}, dataset_product_quantity:{dataset_product_quantity}"
                    )
                
            elif (
                order_row.dataset_azoo_product.product_registration_row.product_type_id
                == 2
            ):  # Premium Dataset(Premium Synthetic Dataset)
                dataset_id_dict["premium_dataset_product"] = (
                    str(dataset_product_dataset_id),
                    dataset_product_quantity,
                )
                logger.info(
                    f"purchase_id:{purchase_id} 에 대응되는 ---> order 테이블의 PREMIUM-dataset_product_dataset_id:{dataset_product_dataset_id}, dataset_product_quantity:{dataset_product_quantity}"
                )

            if order_row.report_azoo_product is None:
                # Common Report 이다.
                logger.info(
                    f"purchase_id:{purchase_id} 에 대응되는 정보 ---> Common Report"
                )
                pass
            else:
                # Premium Report 이다.
                report_product_dataset_id = (
                    order_row.report_azoo_product.product_registration_row.dataset_id
                )
                report_product_quantity = order_row.report_product_quantity
                logger.info(
                    f"purchase_id:{purchase_id} 에 대응되는 정보 ---> order 테이블의 report_product_dataset_id:{report_product_dataset_id}, report_product_quantity:{report_product_quantity}"
                )

                dataset_id_dict["report_product"] = (
                    str(report_product_dataset_id),
                    report_product_quantity,
                )
                pass
            logger.info(
                f"get_dataset_id_by_purchase_id() :: purchase_id:{purchase_id} 의 결과값 --> {dataset_id_dict}"
            )
            return dataset_id_dict
    except:
        logger.error(traceback.format_exc())
        raise


async def update_product_warehouse_table_status(
    db=None,
    product_warehouse_row_id=0,
    status_to="",
    s3_key=None,
    purchase_id=None,
    sold_at=None,
    premium_generated_at=None,
):  # Done
    try:
        logger.info(
            f"update_product_warehouse_table_status() 에 요청이 들어왔습니다. >>>>>> product_warehouse_row_id:{product_warehouse_row_id} , status_to:{status_to} , s3_key:{s3_key} , purchase_id:{purchase_id} , sold_at:{sold_at}"
        )
        async with db.begin():
            subquery_status_to = (
                select(MProductWarehouseStatusModel.id)
                .where(
                    func.lower(MProductWarehouseStatusModel.name) == status_to.lower()
                )
                .subquery()
            )

            # 상태값 업데이트를 수행합니다.
            now_ = datetime.now()

            if (
                premium_generated_at is not None
            ):  # 프리미엄 상품이 다 생성되어서, 생성일(오늘)을 기준으로 expired_at 을 업데이트 함
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.id == product_warehouse_row_id),
                        (
                            (ProductWarehouseModel.status_id <= subquery_status_to)
                            | (ProductWarehouseModel.status_id.is_(None))
                        ),
                    )
                    .values(
                        expired_date=premium_generated_at + timedelta(days=15),
                        status_id=subquery_status_to,
                    )
                )

            elif (
                s3_key is not None
            ):  # s3_key 정보가 있을때, --> 백그라운드 워커가 s3에 업로드 마치고 테이블 update 하는 상황
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.id == product_warehouse_row_id),
                        (
                            (ProductWarehouseModel.status_id < subquery_status_to)
                            | (ProductWarehouseModel.status_id.is_(None))
                        ),
                    )
                    .values(
                        status_id=subquery_status_to, updated_at=now_, s3_key=s3_key
                    )
                )

            elif (
                purchase_id is not None
            ):  # purchase_id 정보가 있을때, --> 고객이 물건을 구매하고, Robert 가 상태 업데이트 api를 호출했을 때.
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.id == product_warehouse_row_id),
                        (
                            (ProductWarehouseModel.status_id < subquery_status_to)
                            | (ProductWarehouseModel.status_id.is_(None))
                        ),
                    )
                    .values(
                        status_id=subquery_status_to,
                        updated_at=now_,
                        purchase_id=purchase_id,
                        sold_at=sold_at,
                        expired_date=sold_at + timedelta(days=7),
                    )
                )

            elif isinstance(
                product_warehouse_row_id, list
            ):  # /noti_canceled 에서 호출한 상황이다. buyer 가 취소하는 상황
                for (
                    product_warehouse_each_row
                ) in (
                    product_warehouse_row_id
                ):  # product_warehouse_row_id 는 실은 list 이다.
                    await db.execute(
                        update(ProductWarehouseModel)
                        .where(
                            (ProductWarehouseModel.id == product_warehouse_each_row),
                            (
                                (ProductWarehouseModel.status_id < subquery_status_to)
                                | (ProductWarehouseModel.status_id.is_(None))
                            ),
                        )
                        .values(status_id=subquery_status_to, updated_at=now_)
                    )
                    pass

            else:  # 백그라운드 워커가 s3로 패키징zip파일을 업로드 하면서, status 업데이트 하는 상황
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(
                        (ProductWarehouseModel.id == product_warehouse_row_id),
                        (
                            (ProductWarehouseModel.status_id < subquery_status_to)
                            | (ProductWarehouseModel.status_id.is_(None))
                        ),
                    )
                    .values(status_id=subquery_status_to, updated_at=now_)
                )
            await db.commit()
            logger.info(
                f"ProductWarehouseModel row_id: {product_warehouse_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"ProductWarehouseModel row_id: {product_warehouse_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_azoo_product_table_status(
    db=None, azoo_product_row_id=0, status_to=""
):  # Done
    try:
        logger.info(
            f"update_azoo_product_table_status() 에 요청이 들어왔습니다. >>>>>> azoo_product_row_id:{azoo_product_row_id} , status_to:{status_to}"
        )
        async with db.begin():
            subquery_statu_to = (
                select(MAzooProductStatusModel.id)
                .where(func.lower(MAzooProductStatusModel.name) == status_to.lower())
                .subquery()
            )

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(AzooProductModel)
                .where(AzooProductModel.id == azoo_product_row_id)
                .where(
                    (AzooProductModel.id == azoo_product_row_id),
                    (
                        (AzooProductModel.status_id < subquery_statu_to)
                        | (AzooProductModel.status_id.is_(None))
                    ),
                )
                .values(status_id=subquery_statu_to, updated_at=datetime.now())
            )
            await db.commit()
            logger.info(
                f"AzooProductModel의 id:{azoo_product_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"AzooProduct Model의 id:{azoo_product_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def get_user_email_addr(db=None, purchase_row_id=0):  # Done
    try:
        logger.info(
            f"get_user_email_addr() 에 요청이 들어왔습니다. >>>>>> purchase_row_id:{purchase_row_id}"
        )
        async with db.begin():
            sub_query = (
                select(PurchaseModel)
                .where(PurchaseModel.id == purchase_row_id)
                .subquery()
            )

            query = select(UserModel.email).where(UserModel.id == sub_query.c.user_id)
            res = await db.execute(query)

            user_email_addr = res.scalar()
            logger.info(
                f"PurchaseModel의 id:{purchase_row_id} 의 user_id 에 해당되는 email을 리턴합니다 --> {user_email_addr}"
            )
            return user_email_addr
    except:
        logger.error(
            f"PurchaseModel의 id:{purchase_row_id} 의 user_id 에 해당되는 email을 얻지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_purchase_table_status(
    db=None, purchase_row_id=0, status_to=""
):  # Done
    try:
        logger.info(
            f"update_purchase_table_status() 에 요청이 들어왔습니다. >>>>>> purchase_row_id:{purchase_row_id} , status_to:{status_to}"
        )
        async with db.begin():

            subquery_status_to = (
                select(MPurchaseStatusModel.id)
                .where(func.lower(MPurchaseStatusModel.name) == status_to.lower())
                .subquery()
            )

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(PurchaseModel)
                .where(
                    (PurchaseModel.id == purchase_row_id),
                    (PurchaseModel.status_id.is_not(None)),
                )
                .values(status_id=subquery_status_to, updated_at=datetime.now())
            )
            await db.commit()
            logger.info(
                f"PurchaseModel의 id:{purchase_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"PurchaseModel의 id:{purchase_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return
