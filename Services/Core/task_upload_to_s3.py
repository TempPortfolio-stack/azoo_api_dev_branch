# celery -A Settings.Worker.celery_worker worker -Q uploading_to_s3_queue -n s3_upload_worker --autoscale=4,2
import os
import os.path
import traceback

from Common.nas_manager import NasManager
from Common.s3_manager import S3Manager
from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import s3_upload_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_upload_to_s3(self, req_info: dict):
    """
    req_info = {
            "sampling_id": sampling_id,
            "mnt_path_in_nas": mnt_path_in_nas,
            "dataset_id" : dataset_id,
            "product_warehouse_id": product_warehouse_id,
            "nums_of_s3_product":nums_of_s3_product,
            "cnt_of_current_azoo_product":cnt_of_current_azoo_product ,
            "azoo_product_id": data.azoo_product_id
        }
    """
    try:
        if req_info["cnt_of_current_azoo_product"] >= req_info["nums_of_s3_product"]:
            logger.info(
                f'cnt_of_current_azoo_product : {req_info["cnt_of_current_azoo_product"]} , nums_of_s3_product : {req_info["nums_of_s3_product"]} 이므로, s3에 업로드 진행하지 않습니다.'
            )
            return

        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        req_info["headers"] = headers
        logger.info(f"received req_info : {req_info}")
        logger.info(f"s3 업로드 작업을 진행합니다. --> {req_info['mnt_path_in_nas']}")

        s3_manager = S3Manager()
        file_base_name = os.path.basename(req_info["mnt_path_in_nas"])
        object_key = f"{settings["DEPLOY_MODE"]}/{req_info['dataset_id']}/azoo_product/{file_base_name}"
        res = s3_manager.data_upload(
            local_file_path=req_info["mnt_path_in_nas"],
            object_key=object_key,
            req_info=req_info,
        )

        if res is None:
            logger.info(f"{req_info['mnt_path_in_nas']} 의 업로드에 실패했습니다.")
            raise Exception(f"s3 업로드 실패")
        else:
            logger.info(f"{req_info['mnt_path_in_nas']} 의 업로드에 성공했습니다.")
            ##### NAS 에서 파일 삭제 해야함.
            nas_manager = NasManager(dataset_id=req_info["dataset_id"])
            nas_manager.delete_target_file(file_path=req_info["mnt_path_in_nas"])
            pass
        return None
    except:
        logger.info(
            f"s3 업로드 작업에 실패하였습니다. ---> sampling_table_row_id: {req_info['sampling_table_row_id']}"
        )
        logger.error(traceback.format_exc())
        return None
    finally:
        if "s3_manager" in locals():
            del s3_manager
        return None
