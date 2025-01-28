# celery -A Settings.Worker.celery_worker worker -Q packaging_queue -n packaging_worker --concurrency=1

import json
import os
import os.path
import tempfile
import traceback
from zipfile import ZipFile

import pandas as pd
import requests

from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import packaging_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_packaging_of_product(self, req_info: dict):
    """
    req_info = {
            "sampling_table_row_id": random_sampled.id,
            "product_registration_id":data.product_registration_id,
            "random_sampled_list" : random_sampled.random_sampled_list,
            "pkl_path": random_sampled.synthetic_task_row.result_files_list_pkl_path,
            "original_dataset_path":random_sampled.product_registration_row.dataset_path,
            "target_cnt_of_zipfiles" : random_sampled.product_registration_row.nums_of_buffer_zip,
            "current_cnt_of_zip_files" : current_packaged_cnt
        }
    """
    try:
        if req_info["current_cnt_of_zip_files"] >= req_info["target_cnt_of_zipfiles"]:
            logger.info(
                f'target_cnt_of_zipfiles : {req_info["target_cnt_of_zipfiles"]} , current_cnt_of_zip_files : {req_info["current_cnt_of_zip_files"]} 이므로, 패키징 진행하지 않습니다.'
            )
            return

        # redis_client = RedisClient()
        # if not redis_client.acquire_packaging_lock():
        #     logger.warning("Lock Key를 얻지 못했습니다. ---> 종료")
        #     return

        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"received req_info : {req_info}")

        logger.info(f"패키징 작업을 진행합니다.")
        zip_file_name = packaging(req_info=req_info)
        if os.path.exists(zip_file_name) == True:
            logger.info(
                f"패키징 작업을 완료하였습니다. ---> sampling_table_row_id: {req_info['sampling_table_row_id']}"
            )
            ######  상태 update 해야 한다. --> sampling table 의 status 를 packaged 로 해야 함.
            update_sampling_table_info = {
                "target_table": "sampling",
                "row_id": req_info["sampling_table_row_id"],
                "status_to": "packaged",
                "mnt_path_in_nas": zip_file_name,
            }
            res = requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_sampling_status",
                json=update_sampling_table_info,
                headers=headers,
            )
            logger.info(
                f"sampling table 의 row_id:{req_info['sampling_table_row_id']} 상태값을 [packaged] 로 update 하도록 api 호출하였습니다."
            )
        return None
    except:
        logger.info(
            f"패키징 작업에 실패하였습니다. ---> sampling_table_row_id: {req_info['sampling_table_row_id']}"
        )
        logger.error(traceback.format_exc())
        return None
    finally:
        # if 'redis_client' in locals():
        #     redis_client.release_packaging_lock()
        #     del redis_client
        pass


def packaging(req_info={}):
    """
    req_info = {
            "sampling_table_row_id": random_sampled.id,
            "product_registration_id":data.product_registration_id,
            "random_sampled_list" : random_sampled.random_sampled_list,
            "pkl_path": random_sampled.synthetic_task_row.result_files_list_pkl_path,
            "original_dataset_path":random_sampled.product_registration_row.dataset_path,
        }
    """
    try:
        sampled_df = pd.read_pickle(req_info["pkl_path"])
        data_format_type = sampled_df.iloc[0]["data_format_type"]
        logger.info(f"패키징할 데이터 타입은 {data_format_type} 입니다.")
        if data_format_type.lower() == "image":
            logger.info(f"img_packaging 호출합니다.")
            return img_packaging(sampled_df=sampled_df, req_info=req_info)
        elif data_format_type.lower() == "tabular":
            logger.info(f"tabular_packaging 호출합니다.")
            return tabular_packaging(sampled_df=sampled_df, req_info=req_info)
    except:
        raise


def tabular_packaging(sampled_df=None, req_info={}):
    try:
        meta_file_path = sampled_df.iloc[0]["meta_json_path"]
        # 임시 디렉토리 없이 ZIP 파일을 생성
        synthetic_task_root = req_info["pkl_path"].split("/sampling.pkl")[0]

        random_sampled_list = json.loads(req_info["random_sampled_list"])
        sampled_df = sampled_df.loc[random_sampled_list]
        meta_file_path = sampled_df.iloc[0]["meta_json_path"]

        zip_file_name = f"{synthetic_task_root}/{req_info['sampling_table_row_id']}.zip"
        with ZipFile(zip_file_name, "w") as zipf:
            # 1. meta.json 파일을 생성하고 추가
            if meta_file_path is not None:
                zipf.write(
                    meta_file_path,
                    os.path.join("product", os.path.basename(meta_file_path)),
                )

            count = 1
            grouped = sampled_df.groupby("synthetic_output_file_path")
            for csv_file_path, group in grouped:
                ith_list = group["ith_row_in_the_output_file"].tolist()
                class_ = group.iloc[0]["class"]
                data_column = group.iloc[0]["columns"]

                temp_df = pd.read_csv(csv_file_path)
                temp_df = temp_df.loc[ith_list]

                # 2. DataFrame을 CSV로 저장하고 ZIP 파일에 추가
                with tempfile.TemporaryDirectory(
                    dir=f"./TmpDir", delete=False
                ) as temp_dir:
                    # df1을 CSV로 저장하고 ZIP 파일에 추가
                    df_path = os.path.join(temp_dir, f"{class_}.csv")
                    temp_df.to_csv(df_path, index=False)
                    zipf.write(
                        df_path,
                        os.path.join("product", "class", class_, f"{class_}.csv"),
                    )
                logger.info(
                    f"{count} / {len(grouped)} 번째 csv 파일 생성중... {csv_file_path}"
                )
                count += 1
                pass
        logger.info(f"Created zip file: {zip_file_name}")
        return zip_file_name
    except:
        raise


def img_packaging(sampled_df=None, req_info={}):
    try:
        # 임시 디렉토리 없이 ZIP 파일을 생성
        synthetic_task_root = req_info["pkl_path"].split("/sampling.pkl")[0]
        random_sampled_list = json.loads(req_info["random_sampled_list"])
        sampled_df = sampled_df.loc[random_sampled_list]
        meta_file_path = sampled_df.iloc[0]["meta_json_path"]

        zip_file_name = f"{synthetic_task_root}/{req_info['sampling_table_row_id']}.zip"  ## sampling table 의 row 값이다.
        with ZipFile(zip_file_name, "w") as zipf:
            # 1. meta.json 파일을 생성하고 추가
            if meta_file_path is not None:
                zipf.write(
                    meta_file_path,
                    os.path.join("product", os.path.basename(meta_file_path)),
                )

            count = 1
            # 2. 기존 파일을 ZIP 파일에 추가
            for index, value in sampled_df.iterrows():
                type_ = value["data_format_type"]
                class_ = value["class"]
                file_path = value["synthetic_output_file_path"]
                arcname = os.path.join(
                    "product", "class", class_, os.path.basename(file_path)
                )
                zipf.write(file_path, arcname)
                logger.info(
                    f"{count} / {len(sampled_df)} 번째 파일 압축 진행중... {file_path}"
                )
                count += 1
                pass
        logger.info(f"Created zip file: {zip_file_name}")
        return zip_file_name
    except:
        raise
