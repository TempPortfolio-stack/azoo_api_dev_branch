# celery -A Settings.Worker.celery_worker worker -Q sampling_from_pkl_queue -n sampling_worker --concurrency=1

import random
import traceback
from datetime import datetime

import pandas as pd
import requests

from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import random_sampling_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_random_sampling_from_pkl(self, req_info: dict):
    """
    req_info = {
            "synthetic_task_row_id":task.id,
            "synthetic_task_id":synthetic_task_id,
            "product_registration_id": task.product_registration_row.id,
            "pkl_path":task.result_files_list_pkl_path,
            "nums_of_buffer_sampling":task.product_registration_row.nums_of_buffer_sampling,
            "nums_of_current_sampling": total_table_items
        }
    """
    try:
        # redis_client = RedisClient()
        # if not redis_client.acquire_sampling_db_lock():
        #     logger.warning("Lock Key를 얻지 못했습니다. ---> 종료")
        #     return

        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"received req_info : {req_info}")

        ###  pkl 파일 읽어온다.
        df = pd.read_pickle(req_info["pkl_path"])
        target_sample_cnt = (
            req_info["nums_of_buffer_sampling"] - req_info["nums_of_current_sampling"]
        )

        logger.info(
            f"현재는 {req_info["nums_of_current_sampling"]} / {req_info["nums_of_buffer_sampling"]} 개가 샘플링 되어 있습니다."
        )
        if req_info["nums_of_current_sampling"] >= req_info["nums_of_buffer_sampling"]:
            logger.info(f"더이상 샘플링 할 필요가 없습니다.")
            pass
        else:
            logger.info(
                f"target sampling count:{target_sample_cnt}  만큼 샘플링 해야합니다. ---> 진행"
            )
            pass

        for i in range(target_sample_cnt):
            sampled_list = sampling(df)
            logger.info(f"{i} --> {sampled_list}")
            insert_req_info = {
                "target_table": "sampling",
                "product_registration_id": req_info["product_registration_id"],
                "generating_synthetic_task_id": str(req_info["synthetic_task_id"]),
                "sampled_list": sampled_list,
            }

            res = requests.post(
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/insert_sampled_list",
                json=insert_req_info,
                headers=headers,
            )

            if res.status_code == 200:
                logger.info(
                    f"product_registration_id: {req_info['product_registration_id']} ,  synthetic_task_id:{req_info['synthetic_task_id']} 의 샘플링리스트가 insert 되었습니다. "
                )
            else:
                logger.error(
                    f"product_registration_id: {req_info['product_registration_id']} ,  synthetic_task_id:{req_info['synthetic_task_id']} 의 샘플링리스트가 insert 실패!! "
                )
                pass

        # return json.dumps(result , indent=2 , ensure_ascii=False).encode("UTF-8")
        return None
    except:
        logger.error(traceback.format_exc())
        return None
    finally:
        # redis_client.release_sampling_db_lock()
        # if 'redis_client' in locals():
        #     del redis_client
        pass


def sampling(df, files_per_zip=10000):
    try:
        #### zip으로 샘플링할 갯수 정하기
        if len(df) >= 1000:
            logger.info(
                f"len(df) == {len(df)} 입니다. --> 1000개 단위로 랜덤샘플링 진행하겠습니다."
            )
            total_data_cnt = 1000
            logger.info(f"files_per_zip:{files_per_zip} , 랜덤샘플링 => {1000}")
            pass
        else:
            #### zip으로 샘플링할 갯수 정하기
            # logger.info(f"len(df) == {len(df)} 입니다. --> dataset의 80% 인 {math.ceil(len(df)*0.8)} 개씩 랜덤 샘플링 진행하겠습니다.")
            # total_data_cnt = math.ceil(len(df) * 0.8)
            # logger.info(f"files_per_zip:{files_per_zip} , math.ceil(len(df) * 0.1 = {math.ceil(len(df) * 0.8)}")

            logger.info(
                f"len(df) == {len(df)} 입니다. --> {len(df)} 개씩 랜덤샘플링 진행하겠습니다."
            )
            total_data_cnt = len(df)
            pass
        # total_data_cnt = min( len(df) , 1000)

        classes = df["class"].unique().tolist()
        num_classes = len(classes)
        base_count = total_data_cnt // num_classes
        remainder = total_data_cnt % num_classes

        distribution = [base_count] * num_classes

        # 남는 엘리먼트를 한 클래스에 추가
        for i in range(remainder):
            distribution[i] += 1

        target_samples_per_class = {}
        for i, class_ in enumerate(classes):
            target_samples_per_class[class_] = distribution[i]
            pass

        logger.info(f"클래스별 추출할 샘플 수 : {target_samples_per_class}")

        # 현재 시간을 시드로 사용
        # current_time_seed = int(datetime.now().timestamp())
        # random_value = random.randint(0, 1000000)
        # current_time_seed += random_value
        # logger.info(f"랜덤시드 for sampling : {current_time_seed}")

        sampled_dfs = []
        for class_value, sample_count in target_samples_per_class.items():
            # 현재 시간을 시드로 사용
            current_time_seed = int(datetime.now().timestamp())
            random_value = random.randint(0, 1000000)
            current_time_seed += random_value
            logger.info(f"랜덤시드 for sampling : {current_time_seed}")
            sampled_dfs.append(
                df[df["class"] == class_value].sample(
                    n=sample_count, random_state=current_time_seed
                )
            )

        # 샘플링된 데이터프레임 결합
        # sampled_df = pd.concat(sampled_dfs).reset_index(drop=True)
        sampled_df = pd.concat(sampled_dfs)
        sampled_list = sampled_df.index.values.tolist()
        logger.info(f"샘플링 완료 되었습니다. ---> {sampled_list}")
        return sampled_list
    except:
        logger.error(traceback.format_exc())
        raise
