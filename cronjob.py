import requests
import asyncio
import json
import time
import traceback

import requests
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# from sqlalchemy.sql.expression import func
from sqlalchemy import func
from sqlalchemy import update
from sqlalchemy.future import select

from Common.redis_token import RedisClient
from Model.models import GeneratingSyntheticTasksModel, MgeneratingSyntheticTaskStatus
from Model.schema import NewTaskSchema
from Services.CureData.api_new_task import toss_new_task
from Settings.Database.database import AsyncSessionLocal
from Settings.Env.env_settings import ENV_STATE
from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import bg_status_worker_logger as status_logger
from Settings.Logger.logging_config import bg_task_retry_logger as retry_logger
from Settings.Logger.logging_config import fastapi_logger as fastapi_logger


async def retry_allocation_task():
    """
    마이클 API 호출에 실패한 task 목록들을 순회하면서, 새롭게 task를 할당시킴
    """
    try:
        async with AsyncSessionLocal() as db:
            # select() 구문을 사용하여 쿼리 생성
            result = await db.execute(
                select(GeneratingSyntheticTasksModel)
                .filter(GeneratingSyntheticTasksModel.task_id.is_(None))
                .order_by(GeneratingSyntheticTasksModel.created_at.asc())
            )
            # 스칼라 값을 추출하여 모든 결과를 리스트로 가져옴
            retry_task_list = result.unique().scalars().all()
            pass

        for task in retry_task_list:
            try:
                if task is not None:
                    # if task.id > 75:
                    retry_logger.info(f"id:{task.id} // task_id:{task.task_id}")

                    params = NewTaskSchema(
                        project_id=task.project_id,
                        project_name="",
                        ai_model_id=task.ai_model_id,
                        dataset_id=task.dataset_id,
                    )
                    await toss_new_task(params=params, new_row_id=task.id)
                    # asyncio.sleep(3)
                    time.sleep(6)
            except:
                print(traceback.format_exc())
                continue
        retry_logger.info("++++++++++++++++++++++++++++++++++++++")
        pass
    except:
        retry_logger.error(traceback.format_exc())
        pass


async def check_generating_synthetictasks_status():
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(GeneratingSyntheticTasksModel)
                .filter(
                    GeneratingSyntheticTasksModel.task_status_id.in_([1])
                    & GeneratingSyntheticTasksModel.is_deleted.is_(False)
                )
                .order_by(GeneratingSyntheticTasksModel.created_at.asc())
            )

            task_list = result.unique().scalars().all()
            pass

        for task in task_list:
            try:
                if task is not None:
                    # if task.id in [113]:
                    status_logger.info(f"id:{task.id} // task_id:{task.task_id}")
                    await call_task_status_check_API(task.task_id)
                    time.sleep(4)

            except:
                status_logger.error(traceback.format_exc())
                continue
        status_logger.info("++++++++++++++++++++++++++++++++++++++")
        pass
    except:
        status_logger.error(traceback.format_exc())


async def call_task_status_check_API(task_id):
    try:
        redis_client = RedisClient()
        access_token = await redis_client.get_access_token()
        if access_token is None:
            del redis_client
            raise Exception("액세스 토큰 발급받지 못했음 --> 예외발생")

        status_logger.info(f"access_token:{access_token}")
        # url = f"http://101.79.8.81:30004/api/v1/service_requests/{task_id}/result_info"
        url = f"{settings['NEW_CUREDATA_URL']}/{task_id}/result_info"

        status_logger.info(
            f"url:{url} // access_token:{access_token} // task_id:{task_id}"
        )
        headers = {"Authorization": access_token}
        response = requests.get(url, headers=headers)

        status_logger.info(f"Status Code : {response.status_code}")
        if response.status_code == 200:
            result = response.json()
            status = result["data"]["status"]
            status = "failed"

            async with AsyncSessionLocal() as db:
                # status_row 검색
                query = (
                    select(MgeneratingSyntheticTaskStatus)
                    .where(
                        func.lower(MgeneratingSyntheticTaskStatus.name)
                        == status.lower()
                    )
                    .limit(1)
                )

                status_result = await db.execute(query)
                status_row = status_result.scalars().first()

                # row 검색
                task_result = await db.execute(
                    select(GeneratingSyntheticTasksModel)
                    .where(GeneratingSyntheticTasksModel.task_id == task_id)
                    .limit(1)
                )
                row = task_result.scalars().first()

                # 데이터베이스 업데이트
                if status_row is not None and row is not None:
                    await db.execute(
                        update(GeneratingSyntheticTasksModel)
                        .where(GeneratingSyntheticTasksModel.task_id == task_id)
                        .values(task_status_id=status_row.id, updated_at=func.now())
                    )
                    await db.commit()
                    status_logger.info(f"task-id:{task_id} status update 성공")
                else:
                    status_logger.error(f"task-id:{task_id} status update 못함")
                pass
            status_logger.info(
                f"JSON Response : {json.dumps(result , indent=4 , ensure_ascii=False)}"
            )
        else:
            raise Exception("Failed to call Michael's curedata creation API")
    except:
        status_logger.error(traceback.format_exc())
    finally:
        if "redis_client" in locals():
            del redis_client


if __name__ == "__main__":
    ### 백그라운드 worker
    scheduler = AsyncIOScheduler(timezone="Asia/Seoul")

    fastapi_logger.warning(
        f"ENV_STATE : {ENV_STATE} // settings['BG_WORKER_ON'] : {settings['BG_WORKER_ON']}"
    )
    if settings["BG_WORKER_ON"] == "true":
        scheduler.start()

        scheduler.add_job(
            check_generating_synthetictasks_status, "interval", seconds=17
        )
        scheduler.add_job(retry_allocation_task, "interval", seconds=7)
        ## main
        try:
            asyncio.get_event_loop().run_forever()
        except KeyboardInterrupt:
            status_logger.warning("KeyboardInterrupt 로 강제 종료!")
            retry_logger.warning("KeyboardInterrupt 로 강제 종료!")
            pass
        except:
            status_logger.warning("Exception 발생!")
            retry_logger.warning("Exception 발생!")
            pass
        pass
    pass
