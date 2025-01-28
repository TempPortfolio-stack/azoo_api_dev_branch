# celery -A Settings.Worker.celery_worker worker -Q prepare_azoo_product_queue -n product_manager --concurrency=1

import concurrent.futures
import traceback

from Common.util import call_api
from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import prepare_product_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_prepare_azoo_product(self, req_info: dict):
    """
    req_info = {
            "azoo_product_id": data.azoo_product_id,
            "product_registration_row_id": product_registration_row_id,
            "generaitng_synthetic_task_id": generaitng_synthetic_task_id,
            "nums_of_buffer_sampling":nums_of_buffer_sampling,
            "nums_of_buffer_zip":nums_of_buffer_zip,
            "nums_of_s3_product":nums_of_s3_product
        }
    """
    try:
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"received req_info : {req_info}")

        logger.info(f"새로운 task 시작>> {req_info}")
        ######### 랜덤샘플링 task 메시지 push  ---> pkl 파일 이용해서, sampling 테이블 채운다.
        random_req_info = {
            "synthetic_task_id": req_info["generaitng_synthetic_task_id"]
        }
        call_api(
            headers=headers,
            method="post",
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/request_random_sampling",
            req_info=random_req_info,
        )
        logger.info(
            f"Step 1) /request_random_sampling => random_req_info:{random_req_info}"
        )

        ######## request_packaging 태스크 메시지 push  ---> sampling 테이블에 있는, 랜덤샘플링 리스트 이용해서, zip 파일 생성한다.
        packaging_req_info = {
            "product_registration_id": req_info["product_registration_row_id"]
        }

        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(int(req_info["nums_of_buffer_zip"])):
                executor.submit(
                    call_api,
                    headers=headers,
                    method="post",
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/request_packaging",
                    req_info=packaging_req_info,
                )
                logger.info(
                    f"Step 2 - [{i+1}번째]) /request_packaging => packaging_req_info:{packaging_req_info}"
                )
                pass
            pass

        ############## sampling 테이블에서 상태값이, packaged 인 row들을(zip파일 만들어진 row들) product_warehouse 테이블에 insert 한다.
        insert_product_warehouse_req_info = {
            "target_table": "product_warehouse",
            "azoo_product_id": req_info["azoo_product_id"],
        }
        call_api(
            headers=headers,
            method="post",
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/insert_product_warehouse",
            req_info=insert_product_warehouse_req_info,
        )
        logger.info(
            f"Step 3) /insert_product_warehouse => insert_product_warehouse_req_info:{insert_product_warehouse_req_info}"
        )

        requst_uploading_req_info = {"azoo_product_id": req_info["azoo_product_id"]}
        with concurrent.futures.ThreadPoolExecutor() as executor:
            for i in range(int(req_info["nums_of_s3_product"])):
                executor.submit(
                    call_api,
                    headers=headers,
                    method="post",
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/azoo_product/request_uploading",
                    req_info=requst_uploading_req_info,
                )
                logger.info(
                    f"Step 4 - [{i+1}번째]) /request_uploading => requst_uploading_req_info:{requst_uploading_req_info}"
                )
                pass
            pass
        return None
    except:
        logger.info(
            f"패키징 작업에 실패하였습니다. ---> sampling_table_row_id: {req_info['sampling_table_row_id']}"
        )
        logger.error(traceback.format_exc())
        return None
