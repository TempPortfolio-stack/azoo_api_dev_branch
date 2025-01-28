import json
import traceback

import redis
import requests

from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import redis_token_manager_logger as logger


class RedisClient:
    _instance = None

    def __new__(cls):
        if not cls._instance:
            host = settings["REDIS_HOST"]
            port = settings["REDIS_PORT"]
            db = settings["REDIS_DB"]
            password = settings["REDIS_PASSWORD"]
            cls._instance = super(RedisClient, cls).__new__(cls)
            cls._instance.redis_conn = redis.Redis(
                host=host, port=port, db=db, password=password
            )
            cls._instance.beat_contract_lock_key = "celery_beat_lock_of_contract"
            cls._instance.task_sampling_lock_key = "celery_task_lock_of_sampling_to_db"
            cls._instance.task_packaging_lock_key = "celery_task_lock_of_packaging"
        return cls._instance

    async def get_access_token(self) -> str | None:
        try:
            ### 1. 레디스에서 access token 가져옴
            cubig_access_token: bytes | None = await self.__get_token_from_redis(
                key="cubig_access_token"
            )
            if cubig_access_token:
                logger.info(f"Redis에서 access 토큰 가져옴!")
                return cubig_access_token.decode("utf-8")

            ### 2. 레디스에 access token 없다 --> refresh 토큰으로 access token 얻어야 함
            cubig_refresh_token: bytes | None = await self.__get_token_from_redis(
                key="cubig_refresh_token"
            )
            if cubig_refresh_token:
                refreshed_token_info: dict = await self.__get_access_token_by_refresh(
                    cubig_refresh_token
                )  # refresh 키로 발급받은 토큰
                if refreshed_token_info:
                    _: bool = await self.__set_token_to_redis(
                        value=refreshed_token_info
                    )
                    logger.info(f"Redis에서 refresh 토큰으로 access 토큰 발급받음")
                    return f'{refreshed_token_info["token_type"]} {refreshed_token_info["access_token"]}'

            ### 3. 레디스에 access 토큰, refresh 토큰 둘 다 없다. --> 아예 새로 발급 받아야 함
            new_access_token_info = await self.__get_access_token_by_new()
            if new_access_token_info:
                _: bool = await self.__set_token_to_redis(value=new_access_token_info)
                logger.info(f"새로 access 토큰 발급받고, Redis에 넣음")
                return f'{new_access_token_info["token_type"]} {new_access_token_info["access_token"]}'
            raise Exception(f"액세스 토큰 얻기에 실패함!!!")
        except:
            logger.error(traceback.format_exc())
            return None

    async def __get_access_token_by_new(self) -> dict | None:
        try:
            url = settings["NEW_TOKEN_URL"]
            data = {
                "client_id": settings["CLIENT_ID"],
                "client_secret": settings["CLIENT_SECRET"],
            }

            response = requests.post(url, json=data)
            logger.info(f"Status Code : {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                logger.info(
                    f"JSON Response : {json.dumps(result , indent=4 , ensure_ascii=False)}"
                )
                access_token = result["data"]["access_token"]  # 유효기간 1시간
                refresh_token = result["data"][
                    "refresh_token"
                ]  # 100일동안 동일 --> 100일 지나면, 아예 새로 토큰을 발급 받아야 함
                token_type = result["data"]["token_type"]
                return {
                    "access_token": access_token,
                    "refresh_token": refresh_token,
                    "token_type": token_type,
                }
            else:
                raise Exception(
                    f"마이클 API 에서 새로운 access토큰 발행 실패 --> status_code:{response.status_code}"
                )
        except:
            logger.error(traceback.format_exc())
            return None

    async def __get_access_token_by_refresh(
        self, cubig_refresh_token: bytes
    ) -> dict | None:
        try:
            url = settings["REFRESH_TOKEN_URL"]
            data = {"refresh_token": cubig_refresh_token.decode("utf-8")}

            response = requests.post(url, json=data)
            logger.info(f"Status Code : {response.status_code}")
            if response.status_code == 200:
                result = response.json()
                logger.info(
                    f"JSON Response : {json.dumps(result , indent=4 , ensure_ascii=False)}"
                )
                access_token = result["data"]["access_token"]
                refresh_token = result["data"]["refresh_token"]
                token_type = result["data"]["token_type"]
            else:
                raise Exception("access토큰 refresh 중 에러 발생")
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": token_type,
            }
        except:
            logger.error(traceback.format_exc())
            return None

    async def __set_token_to_redis(self, value: dict) -> bool:
        try:
            logger.info(f"[REDIS] Set Key -> Value:{value}")

            token_type = value["token_type"]
            access_token = value["access_token"]
            access_token_ttl = 60 * 50  # 60초 * 50분
            self.redis_conn.set(
                "cubig_access_token",
                f"{token_type} {access_token}",
                ex=access_token_ttl,
            )  # access_token set

            refresh_token = value["refresh_token"]
            refresh_token_ttl = 60 * 60 * 24 * 90  # 60초*60분*24시간*90일
            self.redis_conn.set(
                "cubig_refresh_token", refresh_token, ex=refresh_token_ttl
            )  # access_token set
            return True
        except:
            logger.error(f"[REDIS] Set Token to Redis 에러발생")
            raise

    async def __get_token_from_redis(self, key: str) -> bytes | None:
        try:
            val = self.redis_conn.get(key)
            logger.info(f"[REDIS] Get Key:{key} / Value:{val}")
            return val
        except:
            logger.error(f"[REDIS] Get Key:{key} 에러발생")
            return None

    def acquire_contract_api_lock(self):
        try:
            # SETNX 명령어로 락을 설정, 성공 시 True 반환
            is_locked = self.redis_conn.set(
                self.beat_contract_lock_key, "locked", nx=True, ex=None
            )  # nx 설정하면, Redis에 해당 key가 없을때만 생성한다.
            logger.info(f"{self.beat_contract_lock_key} Lock Key를 얻었습니다.")
            return is_locked
        except:
            logger.warning(
                f"{self.beat_contract_lock_key} Lock Key를 얻는데 실패했습니다."
            )
            logger.error(traceback.format_exc())

    def release_contract_api_lock(self):
        try:
            # 락 키 삭제
            self.redis_conn.delete(self.beat_contract_lock_key)
            logger.info(f"{self.beat_contract_lock_key} Lock Key를 해제했습니다.")
        except:
            logger.warning(
                f"{self.beat_contract_lock_key} Lock Key 해제에 실패했습니다."
            )
        pass

    def acquire_sampling_db_lock(self):
        try:
            # SETNX 명령어로 락을 설정, 성공 시 True 반환
            is_locked = self.redis_conn.set(
                self.task_sampling_lock_key, "locked", nx=True, ex=None
            )  # nx 설정하면, Redis에 해당 key가 없을때만 생성한다.
            logger.info(f"{self.task_sampling_lock_key} Lock Key를 얻었습니다.")
            return is_locked
        except:
            logger.warning(
                f"{self.task_sampling_lock_key} Lock Key를 얻는데 실패했습니다."
            )
            logger.error(traceback.format_exc())

    def release_sampling_db_lock(self):
        try:
            # 락 키 삭제
            self.redis_conn.delete(self.task_sampling_lock_key)
            logger.info(f"{self.task_sampling_lock_key} Lock Key를 해제했습니다.")
        except:
            logger.warning(
                f"{self.task_sampling_lock_key} Lock Key 해제에 실패했습니다."
            )

    def acquire_packaging_lock(self):
        try:
            # SETNX 명령어로 락을 설정, 성공 시 True 반환
            is_locked = self.redis_conn.set(
                self.task_packaging_lock_key, "locked", nx=True, ex=None
            )  # nx 설정하면, Redis에 해당 key가 없을때만 생성한다.
            logger.info(f"{self.task_packaging_lock_key} Lock Key를 얻었습니다.")
            return is_locked
        except:
            logger.warning(
                f"{self.task_packaging_lock_key} Lock Key를 얻는데 실패했습니다."
            )
            logger.error(traceback.format_exc())

    def release_packaging_lock(self):
        try:
            # 락 키 삭제
            self.redis_conn.delete(self.task_packaging_lock_key)
            logger.info(f"{self.task_packaging_lock_key} Lock Key를 해제했습니다.")
        except:
            logger.warning(
                f"{self.task_packaging_lock_key} Lock Key 해제에 실패했습니다."
            )
        pass

    def acquire_original_dataset_upload_lock(self):
        try:
            # SETNX 명령어로 락을 설정, 성공 시 True 반환
            is_locked = self.redis_conn.set(
                self.task_original_s3_upload_lock_key, "locked", nx=True, ex=None
            )  # nx 설정하면, Redis에 해당 key가 없을때만 생성한다.
            logger.info(
                f"{self.task_original_s3_upload_lock_key} Lock Key를 얻었습니다."
            )
            return is_locked
        except:
            logger.warning(
                f"{self.task_original_s3_upload_lock_key} Lock Key를 얻는데 실패했습니다."
            )
            logger.error(traceback.format_exc())

    def release_original_dataset_upload_lock(self):
        try:
            # 락 키 삭제
            self.redis_conn.delete(self.task_original_s3_upload_lock_key)
            logger.info(
                f"{self.task_original_s3_upload_lock_key} Lock Key를 해제했습니다."
            )
        except:
            logger.warning(
                f"{self.task_original_s3_upload_lock_key} Lock Key 해제에 실패했습니다."
            )
        pass

    #############  여기부터 Random Sampling 작업임
    def add_sampling_process_list(self, task_id: str) -> bool:
        """
        random_sampling_list 작업요청 리스트에 task_id 를 추가함
        """
        try:
            logger.info(f"[REDIS] Add random_sampling_list task -> task_id:{task_id}")
            self.redis_conn.rpush("random_sampling_list", task_id)  # 오른쪽에 추가
            return True
        except:
            logger.error(
                f"[REDIS] Add random_sampling_list 에러발생 --> task_id:{task_id}"
            )
            raise

    def remove_sampling_process_list(self, task_id: str) -> bool:
        """
        random_sampling_list 리스트에서 작업이 종료된 task_id 를 삭제함
        """
        try:
            logger.info(
                f"[REDIS] Remove random_sampling_list task -> task_id:{task_id}"
            )
            self.redis_conn.lrem("random_sampling_list", 1, task_id)  # 왼쪽에서 제거
            return True
        except:
            logger.error(f"[REDIS] Remove random_sampling_list --> task_id:{task_id}")
            raise

    def get_sampling_process_list(self) -> list:
        """
        random_sampling_list 리스트에 들어있는 task_id 목록을 반환함
        """
        try:
            logger.info(f"[REDIS] Get random_sampling_list task 목록")
            return [
                itm.decode("utf-8")
                for itm in self.redis_conn.lrange("random_sampling_list", 0, -1)
            ]  # 리스트의 모든 원소 반환
        except:
            logger.error(f"[REDIS] Get random_sampling_list task 목록 --> 실패")
            raise

    def is_element_in_sampling_process_list(self, targe_task_id: str) -> bool:
        """
        random_sampling_list 리스트에 target_task_id 가 존재하는지 확인
        """
        try:
            logger.info(
                f"[REDIS] is_exist random_sampling_list --> target_task_id:{targe_task_id}"
            )
            decoded_value = targe_task_id.encode(
                "utf-8"
            )  # 찾고자 하는 문자열을 바이트로 인코딩
            return decoded_value in self.redis_conn.lrange(
                "random_sampling_list", 0, -1
            )
        except:
            logger.error(
                f"[REDIS] is_exist random_sampling_list 확인 실패 --> target_task_id:{targe_task_id}"
            )
            raise

    ################### 여기부터 S3_Uploading 작업임
    def add_s3_uploading_process_list(self, product_warehouse_row_id: int) -> bool:
        """
        s3_uploading_list 리스트에 업로딩 요청 task 을 추가함 :: product_warehouse_id(row_id)
        """
        try:
            logger.info(
                f"[REDIS] Add s3_uploading_list element --> product_warehouse_id:{product_warehouse_row_id}"
            )
            self.redis_conn.rpush(
                "s3_uploading_list", product_warehouse_row_id
            )  # 오른쪽에 추가

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("s3_uploading_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Add s3_uploading_list 에러발생 --> product_warehouse_id:{product_warehouse_row_id}"
            )
            raise

    def remove_s3_uploading_process_list(self, product_warehouse_row_id: int) -> bool:
        """
        s3_uploading_list 리스트에서 product_warehous_id 를 삭제함
        """
        try:
            logger.info(
                f"[REDIS] Remove s3_uploading_list element --> product_warehouse_id:{product_warehouse_row_id}"
            )
            self.redis_conn.lrem(
                "s3_uploading_list", 1, product_warehouse_row_id
            )  # 왼쪽에서 제거

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("s3_uploading_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Remove s3_uploading_list element 에러발생--> product_warehouse_id:{product_warehouse_row_id}"
            )
            raise

    def get_s3_uploading_process_list(self) -> list:
        """
        s3_uploading_list 리스트에 들어있는 product_warehouse_id 목록을 반환함
        """
        try:
            logger.info(f"[REDIS] Get s3_uploading_list element 목록")
            return [
                int(itm.decode("utf-8"))
                for itm in self.redis_conn.lrange("s3_uploading_list", 0, -1)
            ]  # 리스트의 모든 원소 반환
        except:
            logger.error(f"[REDIS] Get s3_uploading_list element 목록 --> 에러발생")
            raise

    def is_element_in_s3_uploading_list(self, product_warehouse_row_id: int) -> bool:
        """
        s3_uploading_list 리스트에 product_warehouse_row_id 가 존재하는지 확인
        """
        try:
            logger.info(
                f"[REDIS] product_warehouse_row_id:{product_warehouse_row_id} 가 s3_uploading_list 에 존재하는지 확인"
            )
            decoded_value = str(product_warehouse_row_id).encode(
                "utf-8"
            )  # 찾고자 하는 문자열을 바이트로 인코딩
            return decoded_value in self.redis_conn.lrange("s3_uploading_list", 0, -1)
        except:
            logger.error(
                f"[REDIS] s3_uploading_list 에 product_warehouse_row_id:{product_warehouse_row_id}  가 존재하는지 확인 --> 에러발생"
            )
            raise

    ################### 여기부터 syn_pkl_file 작업임
    def add_syn_pkl_process_list(self, synthetic_tasks_id: str) -> bool:
        """
        syn_pkl_list 리스트에 피클파일 생성 요청 task 을 추가함 :: synthetic_tasks_id(synthetic_tasks_id)
        """
        try:
            logger.info(
                f"[REDIS] Add syn_pkl_list element --> synthetic_tasks_id:{synthetic_tasks_id}"
            )
            self.redis_conn.rpush("syn_pkl_list", synthetic_tasks_id)  # 오른쪽에 추가

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("syn_pkl_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Add syn_pkl_list 에러발생 --> synthetic_tasks_id:{synthetic_tasks_id}"
            )
            raise

    def remove_syn_pkl_process_list(self, synthetic_tasks_id: str) -> bool:
        """
        syn_pkl_list 리스트에서 synthetic_tasks_id 를 삭제함
        """
        try:
            logger.info(
                f"[REDIS] Remove syn_pkl_list element --> synthetic_tasks_id:{synthetic_tasks_id}"
            )
            self.redis_conn.lrem("syn_pkl_list", 1, synthetic_tasks_id)  # 왼쪽에서 제거

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("syn_pkl_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Remove syn_pkl_list element 에러발생--> synthetic_tasks_id:{synthetic_tasks_id}"
            )
            raise

    def get_syn_pkl_process_list(self) -> list:
        """
        syn_pkl_list 리스트에 들어있는 synthetic_tasks_id 목록을 반환함
        """
        try:
            logger.info(f"[REDIS] Get syn_pkl_list element 목록")
            return [
                itm.decode("utf-8")
                for itm in self.redis_conn.lrange("syn_pkl_list", 0, -1)
            ]  # 리스트의 모든 원소 반환
        except:
            logger.error(f"[REDIS] Get syn_pkl_list element 목록 --> 에러발생")
            raise

    def is_element_in_syn_pkl_list(self, synthetic_tasks_id: str) -> bool:
        """
        syn_pkl_list 리스트에 synthetic_tasks_row_id 가 존재하는지 확인
        """
        try:
            logger.info(
                f"[REDIS] synthetic_tasks_id:{synthetic_tasks_id} 가 syn_pkl_list 에 존재하는지 확인"
            )
            decoded_value = str(synthetic_tasks_id).encode(
                "utf-8"
            )  # 찾고자 하는 문자열을 바이트로 인코딩
            return decoded_value in self.redis_conn.lrange("syn_pkl_list", 0, -1)
        except:
            logger.error(
                f"[REDIS] syn_pkl_list 에 synthetic_tasks_id:{synthetic_tasks_id}  가 존재하는지 확인 --> 에러발생"
            )
            raise

    ################### 여기부터 packaging 작업임
    def add_package_process_list(self, sampling_row_id: int) -> bool:
        """
        package_list 리스트에 패키징요청 task 을 추가함 :: sampling_row_id (sampling_row_id)
        """
        try:
            logger.info(
                f"[REDIS] Add package_list element --> sampling_row_id:{sampling_row_id}"
            )
            self.redis_conn.rpush("package_list", sampling_row_id)  # 오른쪽에 추가

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("package_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Add package_list 에러발생 --> sampling_row_id:{sampling_row_id}"
            )
            raise

    def remove_package_process_list(self, sampling_row_id: int) -> bool:
        """
        package_list 리스트에서 sampling_row_id 를 삭제함
        """
        try:
            logger.info(
                f"[REDIS] Remove syn_pkl_list element --> sampling_row_id:{sampling_row_id}"
            )
            self.redis_conn.lrem("package_list", 1, sampling_row_id)  # 왼쪽에서 제거

            # 리스트에 만료 시간 설정 (혹은 연장)
            self.redis_conn.expire("package_list", 3600)  #
            return True
        except:
            logger.error(
                f"[REDIS] Remove package_list element 에러발생--> sampling_row_id:{sampling_row_id}"
            )
            raise

    def get_package_process_list(self) -> list:
        """
        package_list 리스트에 들어있는 product_registration_id 목록을 반환함
        """
        try:
            logger.info(f"[REDIS] Get package_list element 목록")
            return [
                int(itm.decode("utf-8"))
                for itm in self.redis_conn.lrange("package_list", 0, -1)
            ]  # 리스트의 모든 원소 반환
        except:
            logger.error(f"[REDIS] Get package_list element 목록 --> 에러발생")
            raise

    def is_element_in_package_list(self, sampling_row_id: int) -> bool:
        """
        package_list 리스트에 sampling_row_id 가 존재하는지 확인
        """
        try:
            logger.info(
                f"[REDIS] sampling_row_id:{sampling_row_id} 가 package_list 에 존재하는지 확인"
            )
            decoded_value = str(sampling_row_id).encode(
                "utf-8"
            )  # 찾고자 하는 문자열을 바이트로 인코딩
            return decoded_value in self.redis_conn.lrange("package_list", 0, -1)
        except:
            logger.error(
                f"[REDIS] package_list 에 sampling_row_id:{sampling_row_id}  가 존재하는지 확인 --> 에러발생"
            )
            raise
