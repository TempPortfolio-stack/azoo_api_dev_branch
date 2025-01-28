import os
import logging
import os
import pathlib
from logging import FileHandler
from logging.handlers import TimedRotatingFileHandler

log_directory_path = "Logs"


original_umask = os.umask(0)
pathlib.Path(log_directory_path).mkdir(mode=0o777, parents=True, exist_ok=True)
pathlib.Path("TmpDir").mkdir(mode=0o777, parents=True, exist_ok=True)

# # 임시 파일 저장소 생성
# os.makedirs('TmpDir', exist_ok=True) # 임시 파일 저장소
# # 로그파일 저장소 생성
# os.makedirs(log_directory_path, exist_ok=True)   # 로그 파일 저장소

##  gunicorn에서 사용하는 에러출력 로그 파일 생성
file_path = f"{log_directory_path}/error_log"
with open(file_path, "a") as file:
    pass

##  gunicorn에서 사용하는 접속이력 로그 파일 생성
file_path = f"{log_directory_path}/access_log"
with open(file_path, "a") as file:
    pass


# 원래의 umask로 복원
os.umask(original_umask)


logform = "PName:[%(processName)s] TName:[%(threadName)s] TIME:%(asctime)s LOGLEVEL:[%(levelname)s] FILEPATH:[%(pathname)s] LOGGERNAME[%(name)s]: %(message)s"
formatter = logging.Formatter(logform)
loggingLevel = (
    logging.DEBUG
)  # DEBUG : info,warning,error 나옴 // warning : error만 나옴


def setConsoleLogHandler(logger):
    # 콘솔 핸들러 설정
    global formatter
    global loggingLevel

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    logger.setLevel(loggingLevel)
    return logger


def setFileLogHandler(logger):

    # 파일 핸들러 설정
    global formatter
    global loggingLevel

    file_handler = FileHandler(filename=f"Logs/rotate_{logger.name}.log", mode="a")

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(loggingLevel)
    return logger


def setRotateFileLogHandler(logger):

    # 로테이팅 파일 핸들러 설정
    global formatter
    global loggingLevel

    rotate_file_handler = TimedRotatingFileHandler(
        filename=f"Logs/rotate_{logger.name}.log",
        when="midnight",
        interval=1,  ## midnight의 경우는 사실 interval 정보가 필요없다. 알아서 1day이다.
        backupCount=2,
    )

    rotate_file_handler.setFormatter(formatter)
    logger.addHandler(rotate_file_handler)
    logger.setLevel(loggingLevel)
    return logger


# FastApi 로거
fastapi_logger = logging.getLogger("FastApiLogger")
# fastapi_logger = setFileLogHandler(fastapi_logger)
fastapi_logger = setRotateFileLogHandler(fastapi_logger)


# Util 로거
util_logger = logging.getLogger("UtilLogger")
# util_logger = setFileLogHandler(util_logger)
util_logger = setRotateFileLogHandler(util_logger)


# DB 로거
db_util_logger = logging.getLogger("DbLogger")
# db_util_logger = setFileLogHandler(db_util_logger)
db_util_logger = setRotateFileLogHandler(db_util_logger)


# 백그라운드 상태 체크 워커 로거
bg_status_worker_logger = logging.getLogger("BgStatusWorkerLogger")
# bg_status_worker_logger = setFileLogHandler(bg_status_worker_logger)
bg_status_worker_logger = setRotateFileLogHandler(bg_status_worker_logger)


# 백그라운드 task retry 워커 로거
bg_task_retry_logger = logging.getLogger("BgRetryWorkerLogger")
# bg_task_retry_logger = setFileLogHandler(bg_task_retry_logger)
bg_task_retry_logger = setRotateFileLogHandler(bg_task_retry_logger)


# 레디스 token 관리자 로거
redis_token_manager_logger = logging.getLogger("RedisTokenLogger")
# redis_token_manager_logger = setFileLogHandler(redis_token_manager_logger)
redis_token_manager_logger = setRotateFileLogHandler(redis_token_manager_logger)


# OpenAPI 로거
openapi_logger = logging.getLogger("OpenAPILogger")
# openapi_logger = setFileLogHandler(openapi_logger)
openapi_logger = setRotateFileLogHandler(openapi_logger)


# S3 관리자 로거
s3_manager_logger = logging.getLogger("S3ManagerLogger")
# s3_manager_logger = setFileLogHandler(s3_manager_logger)
s3_manager_logger = setRotateFileLogHandler(s3_manager_logger)


# NAS 관리자 로거
nas_manager_logger = logging.getLogger("NasManagerLogger")
# nas_manager_logger = setFileLogHandler(nas_manager_logger)
nas_manager_logger = setRotateFileLogHandler(nas_manager_logger)


# s3에서 original_dataset NAS로 다운로드 하는 worker
dataset_downloader_logger = logging.getLogger("DatasetDownloadWorkerLogger")
# dataset_downloader_logger = setFileLogHandler(dataset_downloader_logger)
dataset_downloader_logger = setRotateFileLogHandler(dataset_downloader_logger)


# syn_to_pkl 로거
syn_to_pkl_logger = logging.getLogger("Syn2PklWorkerLogger")
# syn_to_pkl_logger = setFileLogHandler(syn_to_pkl_logger)
syn_to_pkl_logger = setRotateFileLogHandler(syn_to_pkl_logger)


# 랜덤샘플링 로거
random_sampling_logger = logging.getLogger("RandomSamplingLogger")
# random_sampling_logger = setFileLogHandler(random_sampling_logger)
random_sampling_logger = setRotateFileLogHandler(random_sampling_logger)


# 패키징 로거
packaging_logger = logging.getLogger("PackagingLogger")
# packaging_logger = setFileLogHandler(packaging_logger)
packaging_logger = setRotateFileLogHandler(packaging_logger)


# s3 업로딩 로거
s3_upload_logger = logging.getLogger("S3UploadingLogger")
# s3_upload_logger = setFileLogHandler(s3_upload_logger)
s3_upload_logger = setRotateFileLogHandler(s3_upload_logger)


# 상품준비 로거
prepare_product_logger = logging.getLogger("PrepareProductLogger")
# prepare_product_logger = setFileLogHandler(prepare_product_logger)
prepare_product_logger = setRotateFileLogHandler(prepare_product_logger)
