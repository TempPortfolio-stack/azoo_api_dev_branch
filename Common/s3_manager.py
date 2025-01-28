import os
import threading
import traceback
from datetime import datetime
from typing import Tuple, Dict, List

import boto3
from boto3.s3.transfer import TransferConfig

from Common.util import call_api
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import s3_manager_logger as logger


class S3Manager:
    __instance = None

    def __new__(cls):
        if cls.__instance is None:

            env = settings["DEPLOY_MODE"]  # 'dev' / 'prod'
            mnt_prefix = settings["NFS_MNT_PREFIX"]
            service_name = "s3"
            endpoint_url = settings["ENDPOINT_URL"]
            region_name = settings["REGION_NAME"]
            access_key = settings["ACCESS_KEY"]
            secret_key = settings["SECRET_KEY"]
            bucket_name = settings["BUCKET_NAME"]

            cls.__instance = super(S3Manager, cls).__new__(cls)
            cls.__instance.bucket_name = bucket_name
            cls.__instance.env = env
            cls.__instance.mnt_prefix = mnt_prefix
            cls.__instance.s3_client = boto3.client(
                service_name,
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region_name,
            )
            pass
        return cls.__instance

    def get_s3_access_url(self, key_prefix) -> List | None:
        try:
            endpoint = settings["ENDPOINT_URL"]
            pre, post = endpoint.split("//")

            object_list = self.__get_object_list(key_prefix)

            result = []
            for itm in object_list:
                if os.path.basename(itm) == "":
                    logger.info(f"{itm} 은 디렉토리 입니다.")
                    continue

                result.append(f"{pre}//azoo.{post}/{itm}")
                pass

            # object_list = [ f"{pre}//azoo.{post}/{itm}" for itm in object_list ]  ## legacy
            logger.info(f"object_list: {result}")
            return result
        except:
            logger.error(traceback.format_exc())
            return None

    def check_file_exists(self, s3_key):
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except:
            logger.error(f"s3_key:{s3_key}는 s3에 존재하지 않는 object 입니다.")
            # logger.error(traceback.format_exc())
            return None

    def create_upload_url(self, key, exp_sec):
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key)
            upload_url = self.s3_client.generate_presigned_url(
                "put_object",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": key,
                },
                ExpiresIn=exp_sec,
            )
            logger.info(f"upload_url 을 생성했습니다. --> {upload_url}")
            return upload_url
        except:
            logger.error(traceback.format_exc())
            return None

    def create_download_url(self, key, exp_sec):
        try:
            target_filename = f"attachment; filename=azoo_data_{datetime.now().strftime('%Y-%m-%d-%H:%M:%S')}.zip"

            download_url = self.s3_client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": self.bucket_name,
                    "Key": key,
                    "ResponseContentDisposition": target_filename,
                    "ResponseContentType": "application/zip",
                },
                ExpiresIn=exp_sec,
            )
            logger.info(f"download_url 을 생성했습니다. {download_url}")
            return download_url
        except:
            logger.error(traceback.format_exc())
            return None

    # def create_download_url(self , key , exp_sec):
    #     try:
    #         download_url = self.s3_client.generate_presigned_url('get_object',
    #                                                 Params={'Bucket': self.bucket_name,
    #                                                         'Key': key},
    #                                                 ExpiresIn=exp_sec)
    #         logger.info(f"download_url 을 생성했습니다. {download_url}")
    #         return download_url
    #     except:
    #         logger.error(traceback.format_exc())
    #         return None

    def delete_object(self, oeject_key):
        try:
            logger.info(f"oeject_key 를 삭제합니다. --> {oeject_key}")
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=oeject_key)
            logger.info(f"oeject_key 를 삭제했습니다. --> {oeject_key}")
            pass
        except:
            logger.error(traceback.format_exc())
            return None
        pass

    def set_public_to_object(self, key_prefix) -> bool | None:
        try:
            object_list = self.__get_object_list(key_prefix=key_prefix)
            for itm in object_list:
                self.__set_acl_to_public(key=itm)
                pass
            return True
        except:
            logger.error(traceback.format_exc)
            return None
        pass

    def __set_acl_to_public(self, key) -> bool | None:
        try:
            self.s3_client.put_object_acl(
                Bucket=self.bucket_name, Key=key, ACL="public-read"
            )
            logger.info(f"[set_acl_to_public] key:{key}")
            return True
        except:
            logger.error(traceback.format_exc())
            return None

    def __get_object_list(self, key_prefix) -> list | None:
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name, Prefix=key_prefix
            )

            object_list = []
            for page in page_iterator:
                object_list += [keys["Key"] for keys in page["Contents"]]
            return object_list
        except:
            logger.error(traceback.format_exc())
            pass
        pass

    def __get_object_list_with_size(self, key_prefix) -> List[Tuple[str, str]] | None:
        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(
                Bucket=self.bucket_name, Prefix=key_prefix
            )

            object_list = []
            for page in page_iterator:
                object_list += [
                    (keys["Key"], keys["Size"]) for keys in page["Contents"]
                ]
            return object_list
        except:
            logger.error(traceback.format_exc())
            pass

    def __generate_upload_post(self, key="", exp_sec=3600) -> Dict[str, str] | None:
        try:
            upload_details = self.s3_client.generate_presigned_post(
                self.bucket_name, key, ExpiresIn=exp_sec
            )
            logger.info(
                f"[generate_upload_post] key:{key} , exp_sec:{exp_sec} , res:{upload_details}"
            )
            return {
                "upload_url": upload_details["url"],
                "key": upload_details["fields"],
            }
        except:
            logger.error(traceback.format_exc())
            return None

    def data_download(self, parent_dir_path_to_save=""):
        try:
            # dir_path = f"{self.mnt_prefix}/{self.env}/{self.dataset_id}/{type}"
            key_prefix = parent_dir_path_to_save.split(f"{self.mnt_prefix}/")[-1]
            type = parent_dir_path_to_save.split("/")[-1]
            MB = 1024 * 1024
            config = TransferConfig(multipart_chunksize=8 * MB)

            logger.info(
                f"parent_dir_path_to_save:{parent_dir_path_to_save} // key_prefix:{key_prefix}  --> 다운로드"
            )

            fail_list = []
            object_list = self.__get_object_list_with_size(key_prefix)
            for key, file_size_byte in object_list:
                try:
                    res = key.split(f"{key_prefix}/")
                    if len(set(res)) == 1:
                        logger.info(
                            f"key:{key} 는 디렉토리 오브젝트입니다. --> continue"
                        )
                        continue

                    file_name = key.split(f"/{type}/")[-1]
                    file_path_to_save = f"{parent_dir_path_to_save}/{file_name}"
                    logger.info(
                        f"file_name:{file_name} / file_path_to_save:{file_path_to_save}"
                    )
                    transfer_callback = TransferCallback(
                        file_size_byte, file_path_to_save
                    )
                    self.s3_client.download_file(
                        Filename=file_path_to_save,
                        Bucket=self.bucket_name,
                        Key=key,
                        Config=config,
                        Callback=transfer_callback,
                    )
                except:
                    fail_list.append(key)
                    ##### failed to download 테이블에 목록을 저장한다. :: background worker 가 천천히 돌면서, download 를 완료한다.
                    logger.error(traceback.format_exc())
                    pass
                finally:
                    if "transfer_callback" in locals():
                        del transfer_callback

            if len(fail_list) > 0:
                logger.warning(f"다운로드 실패 목록: {fail_list}")
                return False
            else:
                logger.info(f"{key_prefix} 의 다운로드가 성공했습니다.")
                return True
        except:
            logger.error(traceback.format_exc())
            return None

    def data_upload(self, local_file_path="", object_key="", req_info=dict()):
        try:
            try:
                logger.info(
                    f"local_file_path:{local_file_path} // object_key:{object_key} --> 업로드"
                )

                ###  업로드 할 파일의 크기 설정
                file_stats = os.stat(local_file_path)
                file_size_byte = file_stats.st_size
                transfer_callback = UploadTransferCallback(
                    file_size_byte, object_key, req_info
                )

                #### 업로드 chunk 크기설정
                MB = 1024 * 1024
                config = TransferConfig(multipart_chunksize=3 * MB)

                self.s3_client.upload_file(
                    Filename=local_file_path,
                    Bucket=self.bucket_name,
                    Key=object_key,
                    Config=config,
                    Callback=transfer_callback,
                )
            except FileNotFoundError:
                logger.info(f"local_file_path: {local_file_path} 가 존재하지 않습니다.")
                pass
            except:
                raise Exception(
                    f"local_file_path: {local_file_path} 의 업로드가 실패했습니다."
                )
        except:
            logger.error(traceback.format_exc())
            return None
        else:
            logger.info(f"{object_key} 의 업로드가 성공했습니다.")
            return True
        finally:
            if "transfer_callback" in locals():
                del transfer_callback

            if "config" in locals():
                del config
        pass

    pass


###### 대용량 파일 다운로드 with callback
class TransferCallback:
    def __init__(self, target_size_byte, file_path_to_save):
        self._target_size = target_size_byte
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}
        self.file_path_to_save = file_path_to_save

    def __call__(self, bytes_transferred):
        thread = threading.current_thread()
        with self._lock:
            self._total_transferred += bytes_transferred
            if thread.ident not in self.thread_info.keys():
                self.thread_info[thread.ident] = bytes_transferred
            else:
                self.thread_info[thread.ident] += bytes_transferred

            target = self._target_size
            logger.info(
                f"{self._total_transferred} of {target} transferred  --> ({(self._total_transferred / target) * 100:.2f}%)"
            )

        if self._total_transferred / target == 1:
            logger.info(f"{self.file_path_to_save} 의 다운로드가 완료되었습니다.")


####### 대용량 파일 업로드
class UploadTransferCallback:
    """
    [Premium Synthetic Dataset 상품의 경우]
    req_info = {
                    "azoo_product_id": azoo_product_id ,
                    "sampling_id" : sampling_table_id,
                    "current_purchase_status_id": sampling_row.product_warehouse_row.purchase_row.status_id ,
                    "mnt_path_in_nas" : sampling_row.mnt_path_in_nas, # Premium_synthetic 의 경우에는.. 랜덤샘플링 , 패키징을 처리하므로.. sampling table에 남는다.
                    "dataset_id" : sampling_row.product_registration_row.dataset_id,
                    "product_warehouse_id" : product_warehouse_row.id ,
                    "product_type_id": 2 , # 2. Premium Synthetic Dataset  ,  3. Premium-Report
                    "data_type_id": sampling_row.product_registration_row.product_base_info_row.data_type_id,  # 1. Synthetic data / 2. DP-based synthetic / 3. Original Data
                    "premium_purchase_id" : premium_purchase_id
                }

    """

    def __init__(self, target_size_byte, object_key, req_info):
        self._target_size = target_size_byte
        self._total_transferred = 0
        self._lock = threading.Lock()
        self.thread_info = {}
        self.object_key = object_key
        self.req_info = req_info
        self.is_begin = False
        pass

    def __call__(self, bytes_transferred):
        try:
            thread = threading.current_thread()
            with self._lock:
                self._total_transferred += bytes_transferred
                if thread.ident not in self.thread_info.keys():
                    self.thread_info[thread.ident] = bytes_transferred
                else:
                    self.thread_info[thread.ident] += bytes_transferred

                target = self._target_size
                logger.info(
                    f"{self._total_transferred} of {target} transferred  --> ({(self._total_transferred / target) * 100:.2f}%)"
                )

                if self.is_begin == False:
                    ### 업로딩
                    #####  상태 update 해야 한다. --> product_warehouse 의 status 를  Uploading to S3 로 해야 함.
                    update_warehouse_table_info = {
                        "target_table": "product_warehouse",
                        "product_warehouse_row_id": self.req_info[
                            "product_warehouse_id"
                        ],
                        "status_to": "Uploading to S3",
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_product_warehouse",
                        req_info=update_warehouse_table_info,
                    )
                    logger.info(
                        f"product_warehouse table 의 row_id:{self.req_info['product_warehouse_id']} 상태값을 [Uploading to S3] 로 update 하도록 api 호출하였습니다."
                    )
                    self.is_begin = True
                else:
                    ### 계속 다운로드 되고 있음 --> 아무것도 안 해도 됨
                    pass

            if self._total_transferred / target == 1:
                logger.info(f"{self.object_key} 의 업로드가 완료되었습니다.")

                #####  상태 update 해야 한다.
                # 1. product_warehouse 의 status 를  Azoo Product 로 해야 함.
                update_warehouse_table_info = {
                    "target_table": "product_warehouse",
                    "product_warehouse_row_id": self.req_info["product_warehouse_id"],
                    "status_to": "Azoo Product",
                    "s3_key": self.object_key,
                }

                call_api(
                    headers=self.req_info["headers"],
                    method="post",
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_product_warehouse",
                    req_info=update_warehouse_table_info,
                )
                logger.info(
                    f"product_warehouse table 의 row_id:{self.req_info['product_warehouse_id']} 상태값을 [Azoo Product] 로 update 하도록 api 호출하였습니다."
                )

                # 3. azoo_product table 의 상태값을 Active 로 변경
                update_azoo_product_table_info = {
                    "target_table": "azoo_product",
                    "azoo_product_row_id": self.req_info["azoo_product_id"],
                    "status_to": "Active",
                }
                call_api(
                    headers=self.req_info["headers"],
                    method="post",
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_azoo_product",
                    req_info=update_azoo_product_table_info,
                )
                logger.info(
                    f"azoo_product table 의 row_id:{self.req_info['azoo_product_id']} 상태값을 [Active] 로 update 하도록 api 호출하였습니다."
                )

                ############### s3 로 업로드가 완료된 이후에 처리될 내용들은 여기에...
                if self.req_info["data_type_id"] == 3:  # 3. Original Data
                    # 할 것 없어!!
                    pass

                elif (
                    self.req_info["product_type_id"] == 1
                ):  # 1. Common Synthetic Dataset
                    # 4. sampling 의 status 를  Moved_to_s3 로 해야 함.
                    update_sampling_table_info = {
                        "target_table": "sampling",
                        "row_id": self.req_info["sampling_id"],
                        "status_to": "Moved_to_s3",
                        "mnt_path_in_nas": None,
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_sampling_status",
                        req_info=update_sampling_table_info,
                    )
                    logger.info(
                        f"sampling table 의 row_id:{self.req_info['sampling_id']} 상태값을 [Moved_to_s3] 로 update 하도록 api 호출하였습니다."
                    )

                elif self.req_info["product_type_id"] == 2:  # 2. Premium Dataset
                    # 프리미엄 데이터셋의 경우 --> s3에 업로드가 되면,

                    # 4. sampling 의 status 를  Moved_to_s3 로 해야 함.
                    update_sampling_table_info = {
                        "target_table": "sampling",
                        "row_id": self.req_info["sampling_id"],
                        "status_to": "Moved_to_s3",
                        "mnt_path_in_nas": None,
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_sampling_status",
                        req_info=update_sampling_table_info,
                    )
                    logger.info(
                        f"sampling table 의 row_id:{self.req_info['sampling_id']} 상태값을 [Moved_to_s3] 로 update 하도록 api 호출하였습니다."
                    )

                    # 5. product_warehouse 테이블에서,  expired_date , updated_at 업데이트 해야 함.
                    # ---> insert_premium_synthetic_product_warehouse() 에서, new_row 를 create 할 때 :: product_id , purchase_id , sampling_id , sold_at 값은 이미 채워지기 때문에.
                    update_warehouse_table_info = {
                        "target_table": "product_warehouse",
                        "product_warehouse_row_id": self.req_info[
                            "product_warehouse_id"
                        ],
                        "status_to": "Sold",
                        "premium_generated_at": str(datetime.now()),
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_product_warehouse",
                        req_info=update_warehouse_table_info,
                    )
                    logger.info(
                        f"product_warehouse table 의 row_id:{self.req_info['product_warehouse_id']} 상태값을 [Sold] , premium_generated_at 을 {datetime.now()} 로 update 하도록 api 호출하였습니다."
                    )

                    # 6. Purchase 테이블의 상태값을 Update 해야 한다.
                    #  self.req_info['current_purchase_status_id']  # 1. Active / 2. Refunded / 3. Premium Pending / 4. Premium Dataset Active / 5. Premium Report Active
                    if self.req_info["current_purchase_status_id"] == 3:
                        # Set to 4 (Premium Dataset Active)
                        update_purchase_table_info = {
                            "purchase_id": self.req_info["premium_purchase_id"],
                            "status_to": "Premium Dataset Active",
                        }
                        call_api(
                            headers=self.req_info["headers"],
                            method="post",
                            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_purchase_status",
                            req_info=update_purchase_table_info,
                        )
                        logger.info(
                            f"purchase table 의 row_id:{self.req_info['premium_purchase_id']} 상태값을 [Premium Dataset Active] 로 update 하도록 api 호출하였습니다."
                        )
                    elif self.req_info["current_purchase_status_id"] == 5:
                        # Set to 1 (Active)
                        update_purchase_table_info = {
                            "purchase_id": self.req_info["premium_purchase_id"],
                            "status_to": "Active",
                        }
                        call_api(
                            headers=self.req_info["headers"],
                            method="post",
                            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_purchase_status",
                            req_info=update_purchase_table_info,
                        )
                        logger.info(
                            f"purchase table 의 row_id:{self.req_info['premium_purchase_id']} 상태값을 [Active] 로 update 하도록 api 호출하였습니다."
                        )

                elif self.req_info["product_type_id"] == 3:  # 3. Premium-Report
                    # 4. sampling 의 status 를  Moved_to_s3 로 해야 함.
                    update_sampling_table_info = {
                        "target_table": "sampling",
                        "row_id": self.req_info["sampling_id"],
                        "status_to": "Moved_to_s3",
                        "mnt_path_in_nas": None,
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_sampling_status",
                        req_info=update_sampling_table_info,
                    )
                    logger.info(
                        f"sampling table 의 row_id:{self.req_info['sampling_id']} 상태값을 [Moved_to_s3] 로 update 하도록 api 호출하였습니다."
                    )

                    # 5. product_warehouse 테이블에서,  expired_date , updated_at 업데이트 해야 함.
                    # ---> insert_premium_synthetic_product_warehouse() 에서, new_row 를 create 할 때 :: product_id , purchase_id , sampling_id , sold_at 값은 이미 채워지기 때문에.
                    update_warehouse_table_info = {
                        "target_table": "product_warehouse",
                        "product_warehouse_row_id": self.req_info[
                            "product_warehouse_id"
                        ],
                        "status_to": "Sold",
                        "premium_generated_at": str(datetime.now()),
                    }
                    call_api(
                        headers=self.req_info["headers"],
                        method="post",
                        url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_product_warehouse",
                        req_info=update_warehouse_table_info,
                    )
                    logger.info(
                        f"product_warehouse table 의 row_id:{self.req_info['product_warehouse_id']} 상태값을 [Sold] , premium_generated_at 을 {datetime.now()} 로 update 하도록 api 호출하였습니다."
                    )

                    # 6. Purchase 테이블의 상태값을 Update 해야 한다.
                    #  self.req_info['current_purchase_status_id']  # 1. Active / 2. Refunded / 3. Premium Pending / 4. Premium Dataset Active / 5. Premium Report Active
                    if self.req_info["current_purchase_status_id"] == 3:
                        # Set to 5 (Premium Report Active)
                        update_purchase_table_info = {
                            "purchase_id": self.req_info["premium_purchase_id"],
                            "status_to": "Premium Report Active",
                        }
                        call_api(
                            headers=self.req_info["headers"],
                            method="post",
                            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_purchase_status",
                            req_info=update_purchase_table_info,
                        )
                        logger.info(
                            f"purchase table 의 row_id:{self.req_info['premium_purchase_id']} 상태값을 [Premium Dataset Active] 로 update 하도록 api 호출하였습니다."
                        )
                    elif self.req_info["current_purchase_status_id"] == 4:
                        # Set to 1 (Active)
                        update_purchase_table_info = {
                            "purchase_id": self.req_info["premium_purchase_id"],
                            "status_to": "Active",
                        }
                        call_api(
                            headers=self.req_info["headers"],
                            method="post",
                            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_purchase_status",
                            req_info=update_purchase_table_info,
                        )
                        logger.info(
                            f"purchase table 의 row_id:{self.req_info['premium_purchase_id']} 상태값을 [Active] 로 update 하도록 api 호출하였습니다."
                        )

        except:
            #### 업로딩 실패함
            #####  상태 update 해야 한다.
            # 1. product_warehouse 의 status 를  Failed during uploading 로 해야 함.
            update_warehouse_table_info = {
                "target_table": "product_warehouse",
                "product_warehouse_row_id": self.req_info["product_warehouse_id"],
                "status_to": "Failed during uploading",
            }
            call_api(
                headers=self.req_info["headers"],
                method="post",
                url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_product_warehouse",
                req_info=update_warehouse_table_info,
            )
            logger.info(
                f"product_warehouse table 의 row_id:{self.req_info['product_warehouse_id']} 상태값을 [Failed during uploading] 로 update 하도록 api 호출하였습니다."
            )
            pass
