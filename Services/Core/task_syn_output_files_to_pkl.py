# celery -A Settings.Worker.celery_worker worker -Q syn_files_to_pkl_queue -n syn_pkl_worker --autoscale=4,2

import os
import os.path
import re
import traceback

import pandas as pd
import requests

from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import syn_to_pkl_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_syn_output_files_to_pkl(self, req_info: dict):
    try:
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        logger.info(f"received req_info : {req_info}")

        synthetic_table_id = req_info[
            "synthetic_table_id"
        ]  ### genarating_synthetic_tasks 테이블 update 하기 위해 사용한다.
        db_update_info = {
            "target_table": "genarating_synthetic_tasks",
            "row_id": synthetic_table_id,
            "result_files_list_pkl_path": None,
            "status_to": "INSERTING_TO_DB",
        }
        requests.post(
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_synthetic_task_status",
            json=db_update_info,
            headers=headers,
        )
        logger.info(f"요청을 보냈습니다. --> db_update_info: {db_update_info}")

        ###  pkl 파일 저장한다.
        synthetic_task_id = req_info[
            "synthetic_task_id"
        ]  ### 이거 이용해서, pkl 파일 저장할 디렉토리이름에 사용한다.
        result_dataset_path = req_info[
            "result_dataset_path"
        ]  ### 이 경로를 이용해서, pkl 을 만든다.
        dataset_format_type = req_info[
            "dataset_format_type"
        ]  ### dataframe 만들때, 사용한다.

        new_df = make_df_of_syn_files(
            target_parent_dir=result_dataset_path,
            data_format_type=dataset_format_type.lower(),
        )
        new_df.to_pickle(f"{result_dataset_path}/sampling.pkl")

        logger.info(f"{result_dataset_path}/sampling.pkl 파일을 생성했습니다.")

        db_update_info = {
            "target_table": "genarating_synthetic_tasks",
            "row_id": synthetic_table_id,
            "result_files_list_pkl_path": f"{result_dataset_path}/sampling.pkl",
            "status_to": "INSERTED_TO_DB",
            "cnt_rows_of_pkl": len(new_df),
        }

        requests.post(
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_synthetic_task_status",
            json=db_update_info,
            headers=headers,
        )
        logger.info(f"요청을 보냈습니다. --> db_update_info: {db_update_info}")

        ######### 랜덤샘플링 task 메시지 push
        # req_info = {
        #                 "synthetic_task_id": str(synthetic_task_id)
        #             }
        # requests.post(url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/request_random_sampling" , json= req_info , headers=headers)
        # logger.info(f"api로 랜덤샘플링 요청을 보냈습니다. --> db_update_info: {db_update_info}")

        # generated_task_id = str(generate_uuid7())
        # task_id = task_random_sampling_from_pkl.apply_async(args=[req_info] , queue="sampling_from_pkl_queue" , task_id=generated_task_id , countdown=0.05)
        # logger.info(f"랜덤샘플링 task를 push 하였습니다. --> req_info: {req_info}")
        # return json.dumps(result , indent=2 , ensure_ascii=False).encode("UTF-8")
        return None
    except:
        logger.error(traceback.format_exc())
        return None


def make_df_of_syn_files(target_parent_dir, data_format_type):
    try:
        """
        id , data_format_type , synthetic_output_file_path , ith_row_in_the_output_file (default=0) , columns  , class , meta_json_path
        """

        # target_parent_dir= "/home/shcho0524/work/image"
        # target_parent_dir= "/home/shcho0524/work/tabular"
        # target_parent_dir = "/mnt/nfs/azoo/data/gen/kaggle/3-1"

        ### 아래 2개가 꼭 입력해야 하는 값이다.
        # data_format_type = "image"
        # root_dir = target_root_path
        # data_format_type = "tabular"

        target_root_path = f"{target_parent_dir}/class"
        root_dir = target_root_path

        my_columns = [
            "data_format_type",
            "synthetic_output_file_path",
            "ith_row_in_the_output_file",
            "columns",
            "class",
            "meta_json_path",
        ]
        new_df = pd.DataFrame(columns=my_columns)

        root_cnt = 0
        meta_json_path = None
        for root, dirs, files in os.walk(root_dir, topdown=True):
            if len(root) > 0:
                logger.info(f"# root : {root}")
                root_cnt += 1
                pass

            if len(files) > 0:
                for file in files:
                    if root_cnt == 1:
                        # root_dir 에 존재하는 파일의 경우... meta.json , 원본 zip파일
                        if file.endswith(".json"):
                            meta_json_path = os.path.join(root, file)
                            logger.info(f"# meta_json_path : {meta_json_path}")
                        pass
                    else:
                        # f-string을 사용하여 정규표현식 패턴 생성
                        pattern = rf"{target_parent_dir}/class/(.*?)/"

                        # 정규표현식 적용
                        path = os.path.join(root, file)
                        match = re.search(pattern, path)
                        if match:
                            current_class = match.group(1)
                            logger.info(f"클래스 정보: {current_class}")
                        else:
                            logger.info(f"클래스 정보를 찾을 수 없습니다. --> {path}")

                        # 클래스 서브디렉토리에 존재하는 파일들
                        if data_format_type == "image":

                            ## 그리드 이미지 확인 및 skip
                            if "_grid." in os.path.basename(path):
                                logger.warning(
                                    f"{path} 는 grid 이미지 이므로 skip 합니다."
                                )
                                continue

                            # logger.info(f"{os.path.join(root, file)} , root_cnt:{root_cnt}")
                            row = {
                                "data_format_type": data_format_type,
                                "synthetic_output_file_path": path,
                                "ith_row_in_the_output_file": 0,  # tabular 의 경우 사용하는 값이다.
                                "columns": [],  # tabular 의 경우 사용하는 값이다.
                                "class": current_class,
                                "meta_json_path": meta_json_path,
                            }
                            logger.info(row)
                            new_df.loc[len(new_df)] = row
                            pass

                        elif data_format_type == "tabular":
                            if file.endswith(".csv"):
                                # logger.info (os.path.join(root, file))
                                syn_df = pd.read_csv(os.path.join(root, file))
                                syn_csv_column = syn_df.columns.values.tolist()

                                for index in syn_df.index.values.tolist():
                                    row = {
                                        "data_format_type": data_format_type,
                                        "synthetic_output_file_path": path,
                                        "ith_row_in_the_output_file": index,  # tabular 의 경우 사용하는 값이다.
                                        "columns": syn_csv_column,  # tabular 의 경우 사용하는 값이다.
                                        "class": current_class,
                                        "meta_json_path": meta_json_path,
                                    }
                                    logger.info(row)
                                    new_df.loc[len(new_df)] = row
                                    pass
                            else:
                                logger.info(
                                    f"{os.path.join(root, file)} 는 tabular 데이터 파일이 아닙니다. skip"
                                )
        # new_df = new_df.reset_index(drop=True)
        return new_df
    except:
        logger.error(traceback.format_exc())
        raise  ## 에러를 상위 호출자로 넘김!
