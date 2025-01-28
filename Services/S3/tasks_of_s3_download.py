# celery -A Settings.Worker.celery_worker worker -Q s3_dataset_download_queue -n download_worker --autoscale=4,2

# 018f7545-0674-7d23-98cc-a07c78c434c5
# 018f7542-f329-736a-a6c1-b24a8d25dbdc

import json
import traceback

import requests

from Common.nas_manager import NasManager
from Common.s3_manager import S3Manager
from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import dataset_downloader_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_s3_download(self, req_info: dict):
    try:
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}

        logger.info(f"received req_info : {req_info}")

        dataset_id = req_info["dataset_id"]
        type = req_info["type"]

        if type == "original_dataset":

            ### 디렉토리 생성
            # update_product_table_status(target_dataset_id=dataset_id , status_to="making_directory_to_nas") # 상태값 변경
            # asyncio.run(update_product_table_status(target_dataset_id=dataset_id , status_to="making_directory_to_nas")) # 상태값 변경
            requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                json={
                    "target_table": "product_registration",
                    "dataset_id": dataset_id,
                    "status_to": "Making directory to NAS",
                },
                headers=headers,
            )

            nas_manager = NasManager(dataset_id=dataset_id)
            dir_path = nas_manager.create_dir(type=type)
            if dir_path is None:
                requests.post(
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                    json={
                        "target_table": "product_registration",
                        "dataset_id": dataset_id,
                        "status_to": "Failed to make directory",
                    },
                    headers=headers,
                )
                raise Exception("error in create_dir()")

            ### 다운로드
            # asyncio.run(update_product_table_status(target_dataset_id=dataset_id , status_to="Downloading to NAS")) # 상태값 변경
            requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                json={
                    "target_table": "product_registration",
                    "dataset_id": dataset_id,
                    "status_to": "Downloding to NAS",
                },
                headers=headers,
            )

            s3_manager = S3Manager()
            res = s3_manager.data_download(parent_dir_path_to_save=dir_path)
            if res is None or res == False:
                requests.post(
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                    json={
                        "target_table": "product_registration",
                        "dataset_id": dataset_id,
                        "status_to": "Failed to download",
                    },
                    headers=headers,
                )
                raise Exception("failed to download")

            #### 다운로드 성공
            # asyncio.run(update_product_table_status(target_dataset_id=dataset_id , status_to="Downloaded to NAS")) # 상태값 변경
            requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                json={
                    "target_table": "product_registration",
                    "dataset_id": dataset_id,
                    "status_to": "Downloaded to NAS",
                },
                headers=headers,
            )

            # # make_zip task 호출함
            # req_to_zip_worker = {
            #     "dataset_id":dataset_id,
            #     "zip_type":"original_dataset_zip",   # zip_type 은 "original_dataset_zip" , "azoo_product_zip" 이 있다.
            # }
            # task_id = task_nas_makezip.apply_async(args=[req_to_zip_worker] , queue="nas_makezip_queue" , countdown=0.05)
            # # value = task_id.get() # 이 부분에서 대기!
            # logger.info(f"task_id:{task_id}의 압축 task가 시작되었습니다.")
        result = {
            "dataset_id": dataset_id,
            "type": type,
            "task_result": res,
        }
        return json.dumps(result, indent=2, ensure_ascii=False).encode("UTF-8")
    except:
        logger.error(traceback.format_exc())
        return None
    finally:
        if "s3_manager" in locals():
            del s3_manager
        if "nas_manager" in locals():
            del nas_manager


# local_file_path = "./552.22-desktop-win10-win11-64bit-international-nsd-dch-whql.exe"
# file_name = "552.22-desktop-win10-win11-64bit-international-nsd-dch-whql.exe"
# object_key = f"large_upload_test/{file_name}"


# file_name = "./552.22-desktop-win10-win11-64bit-international-nsd-dch-whql.exe"
# object_key = f"large_upload_test/{file_name}"
# file_stats = os.stat(file_name)
# file_size_byte = file_stats.st_size


# transfer_callback = TransferCallback(file_size_byte)
# s3_resource = boto3.resource('s3',
#                           endpoint_url=azoo_bucket["endpoint_url"],
#                           aws_access_key_id=azoo_bucket["access_key"],
#                           aws_secret_access_key=azoo_bucket["secret_key"],
#                           region_name=azoo_bucket["region_name"]
#                        )

# s3_client = boto3.client('s3',
#                           endpoint_url=azoo_bucket["endpoint_url"],
#                           aws_access_key_id=azoo_bucket["access_key"],
#                           aws_secret_access_key=azoo_bucket["secret_key"],
#                           region_name=azoo_bucket["region_name"]
#                        )


# # s3_client.upload_file(
# #         Filename = local_file_path, Bucket = azoo_bucket["bucket_name"]  , Key = object_key, Callback=transfer_callback
# #     )
