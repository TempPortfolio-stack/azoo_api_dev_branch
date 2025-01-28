import traceback
from datetime import datetime, timedelta

from sqlalchemy import update, func
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload

from Model.models import (
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
)
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import db_util_logger as logger


# 018f7542-f329-736a-a6c1-b24a8d25dbdc --> id:35


async def get_expired_products_list(db=None):
    try:
        logger.info("get_expired_products_list() 에서 작업 시작합니다. >>>>>>")
        async with db.begin():  ### 트랜젝션

            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            result = await db.execute(
                select(MProductWarehouseStatusModel.id).where(
                    func.lower(MProductWarehouseStatusModel.name) == "Sold".lower()
                )
            )
            sold_status_id = result.scalar()

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
                    & (ProductWarehouseModel.status_id == sold_status_id)
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


async def update_product_table_status(db=None, target_dataset_id="", status_to=""):
    try:
        logger.info("update_product_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"product_registration 테이블의 dataset_id:{target_dataset_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            result = await db.execute(
                select(UploadStatusModel.id).where(
                    func.lower(UploadStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()

            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_upload_status 테이블에 존재하지 않습니다."
                )

            # 현재 upload_status_id 값을 가져옵니다.
            current_status_query = select(
                ProductRegistrationModel.upload_status_id
            ).where(ProductRegistrationModel.dataset_id == target_dataset_id)
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()

            if current_status_id is None:
                raise Exception(
                    f"dataset_id: {target_dataset_id}에 대한 현재 상태를 찾을 수 없습니다."
                )

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"{target_dataset_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.
            if status_to.lower() == "zipped":  ### 정상적으로 zip이 완료되었다.
                await db.execute(
                    update(ProductRegistrationModel)
                    .where(ProductRegistrationModel.dataset_id == target_dataset_id)
                    .values(
                        upload_status_id=status_id,
                        dataset_path=f"/mnt/nfs/azoo/{settings['DEPLOY_MODE']}/{target_dataset_id}",
                        updated_at=datetime.now(),
                    )
                )
            else:
                await db.execute(
                    update(ProductRegistrationModel)
                    .where(ProductRegistrationModel.dataset_id == target_dataset_id)
                    .values(upload_status_id=status_id, updated_at=datetime.now())
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


async def update_contract_table_status(db=None, contract_unique_id="", status_to=""):
    try:
        logger.info("update_contract_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"contract 테이블의 contract_unique_id: {contract_unique_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            result = await db.execute(
                select(MContractStatusModel.id).where(
                    func.lower(MContractStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_upload_status 테이블에 존재하지 않습니다."
                )

            # 현재 contract_status_id 값을 가져옵니다.
            current_status_query = select(ContractModel.contract_status_id).where(
                ContractModel.contract_unique_id == contract_unique_id
            )
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(f"현재의 status 는 {current_status_id}입니다.")

            if current_status_id is None:
                raise Exception(
                    f"contract_unique_id: {contract_unique_id}에 대한 현재 상태를 찾을 수 없습니다."
                )

            # if current_status_id == status_id:
            #     raise Exception(f"contract_unique_id: {contract_unique_id}는 이미 상태값이 {status_to} 입니다.")

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"{contract_unique_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(ContractModel)
                .where(ContractModel.contract_unique_id == contract_unique_id)
                .values(contract_status_id=status_id, updated_at=datetime.now())
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
):
    try:
        logger.info("update_synthetic_task_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"synthetic_tasks 테이블의 row_id:{row_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            # UploadStatusModel에서 name이 status_to인 id를 찾습니다.
            result = await db.execute(
                select(MgeneratingSyntheticTaskStatus.id).where(
                    func.lower(MgeneratingSyntheticTaskStatus.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_genarating_synthetic_task_status 테이블에 존재하지 않습니다."
                )

            # 현재 task_status_id 값을 가져옵니다.
            current_status_query = select(
                GeneratingSyntheticTasksModel.task_status_id
            ).where(GeneratingSyntheticTasksModel.id == row_id)
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(f"현재의 status 는 {current_status_id}입니다.")

            if current_status_id is None:
                raise Exception(
                    f"row_id: {row_id}에 대한 현재 상태를 찾을 수 없습니다."
                )

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"row_id: {row_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(GeneratingSyntheticTasksModel)
                .where(GeneratingSyntheticTasksModel.id == row_id)
                .values(
                    task_status_id=status_id,
                    result_files_list_pkl_path=result_files_list_pkl_path,
                    cnt_rows_of_pkl=cnt_rows_of_pkl,
                    updated_at=datetime.now(),
                )
            )
            await db.commit()
            logger.info(
                f"row_id: {row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(f"row_id: {row_id} 의 상태값을 {status_to}로 변경하지 못했습니다.")
        logger.error(traceback.format_exc())
        return


async def insert_sampled_list_to_table(
    db=None, sampled_list=[], product_registration_row_id=0, synthetic_task_id=""
):
    try:
        logger.info("insert_sampled_list_to_table 에서 작업 시작합니다. >>>>>>")
        logger.info(f"sampling 테이블dp random sampled list 를 insert 하겠습니다.")
        async with db.begin():
            # Step 1: task_id --> task_id's row_id
            result = await db.execute(
                select(GeneratingSyntheticTasksModel.id).where(
                    GeneratingSyntheticTasksModel.task_id == synthetic_task_id
                )
            )
            synthetic_task_table_row_id = result.unique().scalar_one_or_none()
            if synthetic_task_table_row_id is None:
                raise Exception(
                    f" [synthetic_task_id] 가 synthetic_task 테이블에 존재하지 않습니다. "
                )

            # Step 2: 'Sampled'인 MSamplingStatusModel의 id 값을 조회
            result = await db.execute(
                select(MSamplingStatusModel).where(
                    func.lower(MSamplingStatusModel.name) == "Sampled".lower()
                )
            )
            sampling_status = result.unique().scalar_one_or_none()

            if sampling_status is None:
                raise Exception(f" [Sampled] 상태값을 발견하지 못했습니다. ")

            # Step 3: SamplingModel에 데이터 삽입
            new_sampling = SamplingModel(
                created_at=datetime.now(),
                updated_at=datetime.now(),
                is_deleted=False,
                product_registration_id=product_registration_row_id,
                generaitng_synthetic_task_id=synthetic_task_table_row_id,
                status_id=sampling_status.id,
                random_sampled_list=str(sampled_list),  # 여기에 삽입할 값 넣기
            )
            db.add(new_sampling)
            await db.commit()
            # await db.refresh(new_sampling)  # row 새로고침
            # logger.info(f"Inserted new sampling with id: {new_sampling.id}")

        logger.info(f"row_id: {new_sampling.id} 가 insert 되었습니다.")
        return
    except:
        logger.error(
            f"product_registration_id: {product_registration_row_id} , synthetic_task_id:{synthetic_task_id} , {sampled_list} 가 insert 되지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_sampling_table_status(
    db=None, row_id=0, status_to="", mnt_path_in_nas=""
):
    try:
        logger.info("update_sampling_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"sampling 테이블의 row_id:{row_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            result = await db.execute(
                select(MSamplingStatusModel.id).where(
                    func.lower(MSamplingStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_sampling_status 테이블에 존재하지 않습니다."
                )

            # 현재 sampling row의 status 값을 가져옵니다.
            current_status_query = select(SamplingModel.status_id).where(
                SamplingModel.id == row_id
            )
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(f"현재의 status 는 {current_status_id}입니다.")

            if current_status_id is None:
                raise Exception(
                    f"row_id: {row_id}에 대한 현재 상태를 찾을 수 없습니다."
                )

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"row_id: {row_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.
            if mnt_path_in_nas is not None:  ##
                await db.execute(
                    update(SamplingModel)
                    .where(SamplingModel.id == row_id)
                    .values(
                        status_id=status_id,
                        updated_at=datetime.now(),
                        mnt_path_in_nas=mnt_path_in_nas,
                    )
                )
            else:
                await db.execute(
                    update(SamplingModel)
                    .where(SamplingModel.id == row_id)
                    .values(status_id=status_id, updated_at=datetime.now())
                )
                pass

            await db.commit()
            logger.info(
                f"row_id: {row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(f"row_id: {row_id} 의 상태값을 {status_to}로 변경하지 못했습니다.")
        logger.error(traceback.format_exc())
        return


async def insert_product_warehouse_table(db=None, azoo_product_id=0, status=""):
    try:
        logger.info("insert_product_warehouse_table 에서 작업 시작합니다. >>>>>>")
        #####################
        # 1. data.azoo_product_id.product_registration_id 를 가져옴
        #
        # 2. sampling 테이블에서, data.azoo_product_id.product_registration_id 에 해당되는 , status 가 packaged 인 id 를 가져옴
        #
        # 3.
        ####################
        async with db.begin():
            # Step 1: AzooProduct id 에 해당되는 product_registration 테이블의 row_id 가져옴
            logger.info(f"azoo_product_id: {azoo_product_id} 을 검색합니다.")
            # result = await db.execute(select(AzooProductModel).where(AzooProductModel.id == azoo_product_id))
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

            # Step 2: sampling 테이블에서, product_id 에 해당되는 status가 packaged 인 id를 가져옴.
            result = await db.execute(
                select(MSamplingStatusModel.id).where(
                    func.lower(MSamplingStatusModel.name) == "Packaged".lower()
                )
            )
            status_id = result.scalar()
            if status_id is None:
                raise Exception(
                    f"[Packaged] 값은 현재 m_sampling_status 테이블에 존재하지 않습니다."
                )

            # Step 3: product_warehouse 테이블에 이미 등록되어 있는 sampling_id 들을 가져온다.
            result = await db.execute(
                select(ProductWarehouseModel.sampling_id).where(
                    ProductWarehouseModel.product_id == azoo_product_id
                )
            )
            already_registered_id_list = result.scalars()

            # Step 4: Sampling 테이블에서, packaged 상태인 rows 중에서, 이미 product_warehouse 에 등록된 것은 제한 rows 중에서,  target_nums_of_buffer_zip 만큼만 가져온다.
            result = await db.execute(
                select(SamplingModel.id)
                .where(
                    (SamplingModel.status_id == status_id)
                    & (SamplingModel.product_registration_id == product_id)
                    & (SamplingModel.id.not_in(already_registered_id_list))
                )
                .order_by(SamplingModel.created_at.asc())
                .limit(target_nums_of_buffer_zip)
            )

            row_id_list = result.scalars().all()

            # Step 5: ProductWareHouse 에 Insert 하기 위해서, Pending 상태의 status_id 값을 가져온다.
            result = await db.execute(
                select(MProductWarehouseStatusModel.id).where(
                    func.lower(MProductWarehouseStatusModel.name) == status.lower()
                )
            )
            status_id = result.scalar()

            if status_id is None:
                raise Exception(
                    f"[Pending] 값은 현재 m_product_warehouse 테이블에 존재하지 않습니다."
                )

            # Step 6: ProductWareHouse 에 데이터 삽입
            for row_id in row_id_list:
                new_warehouse_product = ProductWarehouseModel(
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    is_deleted=False,
                    sampling_id=row_id,
                    product_id=azoo_product_id,
                    status_id=status_id,
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
):
    try:
        # insert_product_warehouse_table_of_original_dataset(db=db , azoo_product_id=data.azoo_product_id , status = "Pending")
        #####################
        # 1. data.azoo_product_id.product_registration_id 를 가져옴
        #
        # 2. original dataset 형태로 판매되는 상품의 정보를 product_warehouse 테이블에 insert 함.
        ####################
        async with db.begin():
            # Step 1: AzooProduct id 에 해당되는 product_registration 테이블의 row_id 가져옴
            logger.info(
                "insert_product_warehouse_table_of_original_dataset 에서 작업 시작합니다. >>>>>>"
            )
            logger.info(f"azoo_product_id: {azoo_product_id} 을 검색합니다.")
            # result = await db.execute(select(AzooProductModel).where(AzooProductModel.id == azoo_product_id))
            query = (
                select(AzooProductModel)
                .options(
                    selectinload(
                        AzooProductModel.product_registration_row
                    ).selectinload(ProductRegistrationModel.upload_status)
                )
                .where((AzooProductModel.id == azoo_product_id))
            )

            result = await db.execute(query)
            row = result.scalar()
            if row is None:
                raise Exception(
                    f"{azoo_product_id} 값은 현재 azoo_product 테이블에 존재하지 않습니다."
                )

            product_registration_row_id = row.product_id
            product_registration_row = row.product_registration_row
            target_nums_of_buffer_zip = row.product_registration_row.nums_of_buffer_zip

            logger.info(
                f"azoo_product_id:{azoo_product_id} --->  product_registration_id:{product_registration_row_id}"
            )
            if "zipped" not in product_registration_row.upload_status.name.lower():
                logger.error(
                    f"azoo_product_id:{azoo_product_id} --->  product_registration_id:{product_registration_row_id} 는 zip파일이 준비되지 않았습니다."
                )
                raise Exception(
                    f"azoo_product_id: {azoo_product_id} 의 original dataset 의 zip파일이 준비되지 않았습니다."
                )

            # Step 3: product_warehouse 테이블에 이미 azoo_product 의 오리지널 dataset 상품이 등록되어 있는지 확인.
            # result = await db.execute(select(ProductWarehouseModel.id).where(ProductWarehouseModel.product_id == azoo_product_id))
            # already_registered_id = result.scalar()

            # if already_registered_id is not None:
            #     logger.info(f"azoo_product_id:{azoo_product_id} --->  product_registration_id:{product_registration_row_id} 는 이미 product_warehouse 에 등록되어 있습니다.")
            #     return

            # Step 4: ProductWareHouse 에 Insert 하기 위해서, Pending 상태의 status_id 값을 가져온다.
            result = await db.execute(
                select(MProductWarehouseStatusModel.id).where(
                    func.lower(MProductWarehouseStatusModel.name) == status.lower()
                )
            )
            status_id = result.scalar()

            if status_id is None:
                raise Exception(
                    f"[Pending] 값은 현재 m_product_warehouse 테이블에 존재하지 않습니다."
                )

            # Step 6: ProductWareHouse 에 데이터 삽입
            for _ in range(target_nums_of_buffer_zip):
                new_warehouse_product = ProductWarehouseModel(
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    is_deleted=False,
                    sampling_id=None,
                    product_id=azoo_product_id,
                    status_id=status_id,
                )
                db.add(new_warehouse_product)
                pass
            await db.commit()
            logger.info(f"azoo_product_id: {azoo_product_id} 가 insert 되었습니다.")
            return
        pass
    except:
        logger.error(
            f"insert_product_warehouse_table_of_original_dataset 작업 실패했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def get_product_warehouse_row_id(db=None, purchase_id=0, because_of=""):
    try:
        logger.info("get_product_warehouse_row_id 에서 작업 시작합니다. >>>>>>")
        logger.info(f"purchase_id 에 해당되는, product를 매칭하겠습니다.")
        async with db.begin():
            result = await db.execute(
                select(PurchaseModel.azoo_product_id, PurchaseModel.created_at).where(
                    PurchaseModel.id == purchase_id
                )
            )
            if result is None:
                raise Exception(
                    f"purchase_id: {purchase_id}에 해당되는 정보가 존재하지 않습니다. ---> 상품을 준비해야 합니다."
                )

            row = result.first()
            azoo_product_id, sold_at = row

            if because_of == "noti_purchased":
                result = await db.execute(
                    select(MProductWarehouseStatusModel.id).where(
                        func.lower(MProductWarehouseStatusModel.name)
                        == "Azoo Product".lower()
                    )
                )
                status_id = result.scalar()

                ##### 상태값이 Azoo Product 이면서, product_id 가 일치하는 row 가져오기
                query = (
                    select(ProductWarehouseModel.id)
                    .where(
                        (ProductWarehouseModel.status_id == status_id)
                        & (ProductWarehouseModel.purchase_id.is_(None))
                        & (ProductWarehouseModel.product_id == azoo_product_id)
                    )
                    .order_by(ProductWarehouseModel.created_at.asc())
                )

            elif because_of == "noti_canceled":
                result = await db.execute(
                    select(MProductWarehouseStatusModel.id).where(
                        func.lower(MProductWarehouseStatusModel.name) == "Sold".lower()
                    )
                )
                status_id = result.scalar()

                ##### 상태값이 Azoo Product 이면서, product_id 가 일치하는 row 가져오기
                query = (
                    select(ProductWarehouseModel.id)
                    .where(ProductWarehouseModel.purchase_id == purchase_id)
                    .order_by(ProductWarehouseModel.created_at.asc())
                )

            result = await db.execute(query)
            if result is None:
                raise Exception(
                    f"purchase_id: {purchase_id}에 해당되는 상품이 product_warehouse 테이블에 존재하지 않습니다."
                )

            product_warehouse_row_id = result.scalars().first()
            if product_warehouse_row_id is None:
                raise Exception(
                    f"purchase_id: {purchase_id} 에 해당되는 product가 product_warehouse에 존재하지 않습니다."
                )

            logger.info(
                f"purchase_id 와 매칭되는 상품 정보--> product_warehouse_row_id: {product_warehouse_row_id} / sold_at:{sold_at} 리턴합니다."
            )
            return sold_at, product_warehouse_row_id, azoo_product_id
    except:
        logger.error(
            f"purchase_id: {purchase_id} 에 해당되는 product 를 매칭하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        raise


async def get_dataset_id_by_purchase_id(db=None, purchase_id=None):
    try:
        logger.info("get_dataset_id_by_purchase_id 에서 작업 시작합니다. >>>>>>")
        async with db.begin():
            sub_query = (
                select(PurchaseModel).where(PurchaseModel.id == purchase_id).subquery()
            )

            query = (
                select(AzooProductModel)
                .options(selectinload(AzooProductModel.product_registration_row))
                .where(AzooProductModel.id == sub_query.c.azoo_product_id)
            )
            res = await db.execute(query)

            azoo_product_row = res.scalar()

            if azoo_product_row is None:
                raise Exception(
                    f"puchase_id: {purchase_id} 에 해당되는 상품이 azoo_product 테이블에 존재하지 않습니다."
                )
            else:
                dataset_id = azoo_product_row.product_registration_row.dataset_id
                return str(dataset_id)
    except:
        logger.error(
            f"purchase_id:{purchase_id} 에 대응되는 product_registration 의 dataset을 가져오지 못했습니다."
        )
        logger.error(traceback.format_exc())
        raise


async def update_product_warehouse_table_status(
    db=None,
    product_warehouse_row_id=0,
    status_to="",
    s3_key=None,
    purchase_id=None,
    sold_at=None,
):
    try:
        logger.info(
            "update_product_warehouse_table_status 에서 작업 시작합니다. >>>>>>"
        )
        logger.info(
            f"product_warehouse 테이블의 row_id:{product_warehouse_row_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            result = await db.execute(
                select(MProductWarehouseStatusModel.id).where(
                    func.lower(MProductWarehouseStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_product_warehouse 테이블에 존재하지 않습니다."
                )

            # 현재 product_warehouse row의 status 값을 가져옵니다.
            current_status_query = select(ProductWarehouseModel.status_id).where(
                ProductWarehouseModel.id == product_warehouse_row_id
            )
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(f"현재의 status 는 {current_status_id}입니다.")

            if current_status_id is None:
                raise Exception(
                    f"row_id: {product_warehouse_row_id}에 대한 현재 상태를 찾을 수 없습니다."
                )

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"row_id: {product_warehouse_row_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.
            now_ = datetime.now()

            if (
                s3_key is not None
            ):  # s3_key 정보가 있을때, --> 백그라운드 워커가 s3에 업로드 마치고 테이블 update 하는 상황
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(ProductWarehouseModel.id == product_warehouse_row_id)
                    .values(status_id=status_id, updated_at=now_, s3_key=s3_key)
                )
            elif (
                purchase_id is not None
            ):  # purchase_id 정보가 있을때, --> 고객이 물건을 구매하고, Robert 가 상태 업데이트 api를 호출했을 때.
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(ProductWarehouseModel.id == product_warehouse_row_id)
                    .values(
                        status_id=status_id,
                        updated_at=now_,
                        purchase_id=purchase_id,
                        sold_at=sold_at,
                        expired_date=sold_at + timedelta(days=7),
                    )
                )
                pass
            else:  # 백그라운드 워커가 s3로 패키징zip파일을 업로드 하면서, status 업데이트 하는 상황 &&&&&  buyer 가 취소한 경우.
                await db.execute(
                    update(ProductWarehouseModel)
                    .where(ProductWarehouseModel.id == product_warehouse_row_id)
                    .values(status_id=status_id, updated_at=now_)
                )
            await db.commit()
            logger.info(
                f"row_id: {product_warehouse_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"row_id: {product_warehouse_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_azoo_product_table_status(
    db=None, azoo_product_row_id=0, status_to=""
):
    try:
        logger.info("update_azoo_product_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"azoo_product 테이블의 row_id:{azoo_product_row_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():
            result = await db.execute(
                select(MAzooProductStatusModel.id).where(
                    func.lower(MAzooProductStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_azoo_product_status 테이블에 존재하지 않습니다."
                )

            # 현재 product_warehouse row의 status 값을 가져옵니다.
            current_status_query = select(AzooProductModel.status_id).where(
                AzooProductModel.id == azoo_product_row_id
            )
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(f"현재의 status 는 {current_status_id}입니다.")

            if current_status_id is None:
                # raise Exception(f"row_id: {azoo_product_row_id}에 대한 현재 상태를 찾을 수 없습니다.")
                ## 초기값으로 null 을 설정한 것이므로, current_status_id = 1 로 간주한다.
                current_status_id = 1
                pass

            # 현재 상태값과 status_id 값을 비교합니다.
            if current_status_id >= status_id:
                logger.info(
                    f"row_id: {azoo_product_row_id}의 현재 상태값({current_status_id})이 {status_to}({status_id})보다 크거나 같으므로, 업데이트를 수행하지 않습니다."
                )
                return

            # 상태값 업데이트를 수행합니다.

            await db.execute(
                update(AzooProductModel)
                .where(AzooProductModel.id == azoo_product_row_id)
                .values(status_id=status_id, updated_at=datetime.now())
            )
            await db.commit()
            logger.info(
                f"row_id: {azoo_product_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"row_id: {azoo_product_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def update_purchase_table_status(db=None, purchase_row_id=0, status_to=""):
    try:
        logger.info("update_purchase_table_status 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"purchase 테이블의 row_id:{purchase_row_id} 의 상태를 {status_to} 로 변경하겠습니다."
        )
        async with db.begin():

            # 현재 purchase row의 status 값을 가져옵니다.
            current_status_query = select(PurchaseModel.status_id).where(
                PurchaseModel.id == purchase_row_id
            )
            current_status_result = await db.execute(current_status_query)
            current_status_id = current_status_result.scalar()
            logger.info(
                f"현재의 purchase_id:{purchase_row_id}의  status 는 {current_status_id}입니다."
            )

            if current_status_id is None:
                raise Exception(
                    f"purchase_row_id: {purchase_row_id}에 대한 현재 상태를 찾을 수 없습니다. ---> 현재, purchase_id:{purchase_row_id} 값이 테이블에 존재하지 않습니다. --> raise"
                )

            result = await db.execute(
                select(MPurchaseStatusModel.id).where(
                    func.lower(MPurchaseStatusModel.name) == status_to.lower()
                )
            )
            status_id = result.scalar()
            logger.info(f"바꿔야할 status{status_to} 는 {status_id}입니다.")
            if status_id is None:
                raise Exception(
                    f"{status_to} 값은 현재 m_purchase_status 테이블에 존재하지 않습니다."
                )

            # 상태값 업데이트를 수행합니다.
            await db.execute(
                update(PurchaseModel)
                .where(PurchaseModel.id == purchase_row_id)
                .values(status_id=status_id, updated_at=datetime.now())
            )
            await db.commit()
            logger.info(
                f"purchase_row_id: {purchase_row_id} 의 상태를 {status_to} 로 성공적으로 변경하였습니다."
            )
            return
    except:
        logger.error(
            f"purchase_row_id: {purchase_row_id} 의 상태값을 {status_to}로 변경하지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return


async def get_user_email_addr(db=None, purchase_row_id=0):
    try:
        logger.info("get_user_email_addr 에서 작업 시작합니다. >>>>>>")
        logger.info(
            f"purchase 테이블의 row_id:{purchase_row_id} 의 user_id 의 email 주소를 찾습니다."
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
            return user_email_addr
    except:
        logger.error(
            f"purchase_row_id: {purchase_row_id} 의 user_id 에 해당되는 email을 얻지 못했습니다."
        )
        logger.error(traceback.format_exc())
        return
