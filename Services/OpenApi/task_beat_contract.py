# celery -A Settings.Worker.celery_worker beat   # 이렇게 beat 를 실행시켜줘야 한다. 이게 task를 발행하는 역할을 한다.
# celery -A Settings.Worker.celery_worker worker -Q celery_periodic_queue -n beat_contract --concurrency=1
import json
import traceback
from datetime import datetime, timedelta

import requests

from Common.redis_token import RedisClient
from Common.util import get_azoo_fastapi_access_token
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import openapi_logger as logger
from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_contract_status_check(self):
    try:
        redis_client = RedisClient()

        if not redis_client.acquire_contract_api_lock():
            logger.warning("Lock Key를 얻지 못했습니다. ---> 종료")
            return

        today = datetime.today()
        from_ = today + timedelta(-10)  # 과거 10일동안의
        to_ = today + timedelta(2)

        url = settings["GLOSIGN_CHECK_CONTRACT_URL"]
        params = {
            "from": from_.strftime("%Y%m%d"),
            "to": to_.strftime("%Y%m%d"),
            "clientid": settings["GLOSIGN_CLIENT_ID"],
        }

        headers = {
            "Authorization": f"Bearer {settings['GLOSIGN_ACCESS_TOKEN_API_KEY']}"
        }
        response = requests.get(url, headers=headers, params=params)

        logger.info(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            res = response.json()
            logger.warning(f"링크계약 조회가 완료되었습니다.")
            logger.info(
                f"JSON Response: {json.dumps(res , indent=4 , ensure_ascii=False)}"
            )
            completed_list = get_completed_contract_list(res)
            logger.info(f"서명 완료된 contract_list : {completed_list}")

            access_token = get_azoo_fastapi_access_token()
            if access_token is None:
                raise Exception(
                    "현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다."
                )

            headers = {"Authorization": f"Bearer {access_token}"}
            for contract_id in completed_list:
                requests.post(
                    url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/core/update_contract_status",
                    json={
                        "target_table": "contract",
                        "contract_id": contract_id,
                        "status_to": "Contracted",
                    },
                    headers=headers,
                )

                logger.info(
                    f"contract: {contract_id} 의 상태를 Contracted로 변경요청하였습니다."
                )
                pass
            pass
        else:
            res = response.json()
            logger.warning(res)
            logger.warning(
                f"링크계약 조회가 실패했습니다. --> status_code:{response.status_code}"
            )
    except:
        logger.error(traceback.format_exc())
        return None
    finally:
        redis_client.release_contract_api_lock()
        if "redis_client" in locals():
            del redis_client


def get_completed_contract_list(response):
    try:
        contracted_list = []
        for itm in response:
            if itm["contract_count"] > 0:
                contracted_list.append(itm["id"])
            pass
        return contracted_list
    except:
        logger.error(traceback.format_exc())
        pass
