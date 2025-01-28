# celery -A Settings.Worker.celery_worker worker -Q nas_makezip_queue -n zip_worker --autoscale=4,2

import json
import traceback

import requests

from Common.nas_manager import NasManager
from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import dataset_downloader_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_nas_makezip(self, req_info: dict):
    try:
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"received req_info : {req_info}")

        dataset_id = req_info["dataset_id"]
        zip_type = req_info["zip_type"]
        nas_manager = NasManager(dataset_id=dataset_id)

        if zip_type == "original_dataset_zip":
            ### original_dataset zip파일 만들기
            # asyncio.run(update_product_table_status(target_dataset_id=dataset_id , status_to="Zipping")) # 상태값 변경
            requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                json={
                    "target_table": "product_registration",
                    "dataset_id": dataset_id,
                    "status_to": "Zipping",
                },
                headers=headers,
            )

            res = nas_manager.make_zip_file(zip_type="original_dataset_zip")

            if res is None:
                requests.post(
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                    json={
                        "target_table": "product_registration",
                        "dataset_id": dataset_id,
                        "status_to": "Failed to zip",
                    },
                    headers=headers,
                )
                raise Exception("압축파일 task 실패")
            else:
                # asyncio.run(update_product_table_status(target_dataset_id=dataset_id , status_to="Zipped")) # 상태값 변경
                requests.post(
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_upload_status",
                    json={
                        "target_table": "product_registration",
                        "dataset_id": dataset_id,
                        "status_to": "Zipped",
                    },
                    headers=headers,
                )
                pass

        result = {"dataset_id": dataset_id, "zip_type": zip_type, "result": res}
        return json.dumps(result, indent=2, ensure_ascii=False).encode("UTF-8")
    except:
        logger.error(traceback.format_exc())
        return None
    finally:
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
