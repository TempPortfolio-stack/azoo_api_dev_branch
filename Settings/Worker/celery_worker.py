import os

from celery import Celery
from celery.schedules import crontab
from kombu import Queue

CELERY_BROKER_URL = os.environ.get("CELERY_BROKER_URL")


celery_app = Celery(
    "my_celery",
    broker=CELERY_BROKER_URL,
    # 큐생성
    queues=[
        Queue("s3_dataset_download_queue", routing_key="s3_dataset_download_queue"),
        Queue("nas_makezip_queue", routing_key="nas_makezip_queue"),
        Queue("syn_files_to_pkl_queue", routing_key="syn_files_to_pkl_queue"),
        Queue("sampling_from_pkl_queue", routing_key="sampling_from_pkl_queue"),
        Queue("packaging_queue", routing_key="packaging_queue"),
        Queue("uploading_to_s3_queue", routing_key="uploading_to_s3_queue"),
        Queue("prepare_azoo_product_queue", routing_key="prepare_azoo_product_queue"),
        Queue("celery_periodic_queue", routing_key="celery_periodic_queue"),
        Queue(
            "celery_periodic_queue_for_remove_s3",
            routing_key="celery_periodic_queue_for_remove_s3",
        ),
        # Queue("celery_periodic_queue_for_premium" , routing_key="celery_periodic_queue_for_premium"),
    ],
)

#### TimeZone
celery_app.conf.timezone = "Asia/Seoul"


celery_app.conf.update(
    imports=[
        "Services.S3.tasks_of_s3_download",
        "Services.S3.tasks_of_nas_makezip",
        "Services.Core.task_syn_output_files_to_pkl",  # synthetic tasks 결과파일 pkl 로 저장해서, path update
        "Services.Core.task_random_sampling",  # pkl 파일 읽어서 랜덤샘플링 & sampling table 에 insert
        "Services.Core.task_packaging",  # sampling table 정보로 상품으로 패키징
        "Services.Core.task_upload_to_s3",  # 패키징된 zip파일을 s3 로 업로드
        "Services.Core.task_prepare_azoo_product",  # azoo_product 상품을 등록하는 전체 과정을 호출함( 랜덤샘플링 , 패키징 , insert product_warehouse , upload to s3)
        "Services.OpenApi.task_beat_contract",  # beat
        "Services.Core.task_beat_remove_expired",  # beat
        # "Services.Core.task_beat_premium", # beat
    ],
    result_expires=3600,
    result_serializer="json",
    accept_content=["json", "yaml"],
    broker_connection_retry_on_startup=True,
)


# Celery Beat 설정
celery_app.conf.beat_schedule = {
    "task-contract-status-update": {
        "task": "Services.OpenApi.task_beat_contract.task_contract_status_check",
        "schedule": crontab(hour=1, minute=30),  # 크론탭 일정대로 호출함
        "options": {"queue": "celery_periodic_queue"},
    },
    "task-s3-expired-remove": {
        "task": "Services.Core.task_beat_remove_expired.task_remove_expired",
        "schedule": crontab(hour=1, minute=50),  # 3600, # 3600초에 한 번 호출함
        # "schedule": crontab(hour=15 , minute=52),  #3600, # 3600초에 한 번 호출함
        "options": {"queue": "celery_periodic_queue_for_remove_s3"},
    },
    # "task-premium-product-update": {
    #     "task": "Services.Core.task_beat_premium.task_premium_product_update",
    #     "schedule": crontab(hour=3 , minute=30),
    #     # "schedule": crontab(hour=15 , minute=52),  #3600, # 3600초에 한 번 호출함
    #     "options": {'queue' : 'celery_periodic_queue_for_premium_product'},
    #     },
}
