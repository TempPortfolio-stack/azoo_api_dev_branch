from datetime import datetime
from typing import Generic, List, TypeVar
from uuid import UUID

from fastapi import Query
from pydantic import BaseModel, ConfigDict

from Settings.Env.env_settings import settings


class LoginSchema(BaseModel):
    user: str = Query(
        ..., description=f"토큰을 발급받을 유저 : {settings['ALLOWED_USER']}"
    )
    pass


class DbConnectInfoSchema(BaseModel):
    type: str = Query(..., description="네이버 or 아마존 구분값")
    bucket_name: str = Query(..., description="S3 bucket name")
    access_key: str = Query(..., description="s3 access key")
    secret_key: str = Query(..., description="s3 secret key")


class AdminBeginCureSchema(BaseModel):
    project_id: int = Query(..., description="내가 관리하는 project 테이블의 ID값")
    project_name: str = Query(
        ..., description="내가 관리하는 project 테이블의 project_name 값"
    )
    dataset_id: str = Query(
        ...,
        description="마이클이 관리하는 데이터 저장위치 테이블의 id값 DataSet 테이블의 'id'컬럼 ",
    )  # 마이클의 DB :: DataSet 테이블의 id
    ai_model_id: str = Query(..., description="마이클이 관리하는 AI모델 테이블의 id값")
    ai_model_name: str = Query(
        ..., description="마이클이 관리하는 AI모델 테이블의 모델이름"
    )
    db_connection_info: DbConnectInfoSchema = Query(
        ..., description="CureData 완료후 결과 데이가 저장될 데이터베이스 연결정보"
    )


class SellerNotiUploadedSchema(BaseModel):
    contract_id: int = Query(
        ..., description="Robert가 관리하는 테이블의 conrtact_id 값 "
    )
    dataset_id: UUID = Query(
        "",
        description="마이클이 관리하는 데이터 저장위치 테이블의 id값 DataSet 테이블의 'id'컬럼 ",
    )  # 마이클의 DB :: DataSet 테이블의 id
    is_deleted: bool = Query(False, description="is deleted? --> default:False")

    class Config:
        from_attributes = True


class NewTaskSchema(BaseModel):
    project_id: int = Query(..., description="id of Projects table")
    project_name: str = Query(..., description="project_name of Projects table")
    dataset_id: UUID = Query(..., description="dataset_id for running task")
    ai_model_id: UUID = Query(..., description="ai_model_it for running task")
    premium_purchase_id: int | None = Query(
        default=None, description="[Premium] purchase_id"
    )
    # dataset_path:str = Query(...,  description="product_registration 테이블의 dataset_path")
    # db_connection_info:dict = Query(..., description="db_connection_info for storing result dataset")

    class Config:
        from_attributes = True


class TaskSchema(BaseModel):
    id: int = Query(..., description="genarating_synthetic_tasks 테이블의 ID")
    # project_id:int = Query(..., description="Projects")
    ai_model_id: UUID = Query(..., description="curedata 생성에 사용하는 AI 모델")
    dataset_id: UUID = Query(..., description="curedata 생성에 사용하는 dataset")
    db_connection_info: dict = Query(
        ..., description="curedata 결과가 저장될 storage 서버의 접속 정보"
    )
    task_id: UUID | str | None = Query(
        ...,
        description="curedata 생성 task 를 구분하는 ID값, 나중에 download 할때 이 값을 Michael에게 넘겨줘야 함",
    )
    task_status_id: int = Query(..., description="curedata 생성 task의 상태값 id")
    dataset_score: str = Query(
        ..., description="curedata에서 생성된 dataset의 accuracy"
    )
    dataset_report_html_file_url: str = Query(
        ..., description="리포트 html 파일에 접근 가능한 url"
    )
    created_at: datetime = Query(..., description="task 생성일자")
    updated_at: datetime = Query(..., description="task 업데이트일자")
    is_deleted: bool = Query(..., description="")

    model_config = ConfigDict(from_attributes=True)


class DbconnectionListSchema(BaseModel):
    id: int = Query(..., description="genarating_synthetic_tasks 테이블의 ID")
    admin_name: str = Query(
        ..., description="admin user who operates curedata creation"
    )
    dataset_id: UUID = Query(
        ..., description="datasetID that is used for curedata creation"
    )
    data_type: str = Query(..., description="dataset type")
    data_format: str = Query(..., description="dataset format")
    created_at: datetime = Query(
        ..., description="the time when the curedata creation begins"
    )
    pass


class IsConnectedSchema(BaseModel):
    service_name: str = Query(
        ..., description="if naver --> 'NAVER'  / if amazon --> 'AMAZON'"
    )
    access_key: str = Query(..., description="access key")
    secret_key: str = Query(..., description="secret key")
    bucket_name: str = Query(..., description="bucket name")


class S3GetUploadUrlSchema(BaseModel):
    dataset_id: str = Query(..., description="UUID")
    type: str = Query(..., description="sample , original_dataset")
    source_file_name: str = Query(..., description="ex) a.jpg   b.txt    c.pdf")


class S3NotiUploadDoneSchema(BaseModel):
    dataset_id: str = Query(..., description="UUID")
    type: str = Query(..., description="sample , original_dataset")


class NasReqOriginalDatasetZip(BaseModel):
    dataset_id: str = Query(..., description="UUID")
    zip_type: str = Query(..., description="sample , original_dataset")


class RandomSamplingSchema(BaseModel):
    synthetic_task_id: UUID = Query(
        ..., description="synthetic_tasks_id of synthetic_task table"
    )


class PackagingSchema(BaseModel):
    product_registration_id: int = Query(
        ..., description="row_id of product_registration table"
    )
    synthetic_task_id: UUID | None = Query(
        default=None, description="task_id of generating_synthetic_tasks table"
    )
    synthetic_task_row_id: int | None = Query(
        default=None, description="row_id of generating_synthetic_tasks table"
    )
    is_premium_report: bool | None = Query(
        default=None, description="is premium report"
    )
    new_sampling_row_id: int | None = Query(
        default=None,
        description="Premium Synthetic Dataset 의 경우.. sampling 테이블에 insert된 row_id 정보가 필요하다.",
    )


class UploadAzooProductSchema(BaseModel):
    azoo_product_id: int = Query(..., description="row_id of azoo_product table")
    premium_purchase_id: int | None = Query(
        default=None, description="row_id of purchase table"
    )
    sampling_table_id: int | None = Query(
        default=None, description="row_id of sampling table"
    )
    synthetic_tasks_id: UUID | None = Query(
        default=None, description="task_id of generating_synthetic_tasks table"
    )


class UploadateUploadStatusSchema(BaseModel):
    dataset_id: str = Query(..., description="UUID")
    status_to: str = Query(..., description="wanna status to this")


class UploadateContractStatusSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    contract_id: str = Query(..., description="contract unique id")
    status_to: str = Query(..., description="wanna status to this")


class UploadateSynTaskStatusSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    row_id: int = Query(..., description="row id of ")
    result_files_list_pkl_path: str | None = Query(
        default=None, description="output pkl file path"
    )
    status_to: str = Query(..., description="wanna status to this")
    cnt_rows_of_pkl: int | None = Query(
        default=None, description="length of the dataframe rows"
    )


class InsertSamplingListSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    product_registration_id: int = Query(
        ..., description="row id of product_registration table"
    )
    generating_synthetic_task_id: UUID = Query(
        ..., description="row id of synthetic task table"
    )
    sampled_list: list = Query(..., description="random sampled list")


class InsertWareHouseSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    azoo_product_id: int = Query(..., description="row id of azoo_product table")
    premium_purchase_id: int | None = Query(
        default=None, description="premium_purchase_id"
    )
    sampling_table_row_id: int | None = Query(
        default=None, description="sampling_table_row_id"
    )


class UpdateWareHouseSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    product_warehouse_row_id: int = Query(
        ..., description="row id of product_warehouse table"
    )
    status_to: str = Query(..., description="wanna status to this")
    s3_key: str | None = Query(default=None, description="s3 object_key")
    purchase_id: int | None = Query(
        default=None, description="row id of purchase table"
    )
    premium_generated_at: datetime | None = Query(
        default=None, description="generated at == datetime.now()"
    )


class NotiPurchasedSchema(BaseModel):
    purchase_id: int = Query(..., description="row id of purchase table")


class UpdatePurchaseSchema(BaseModel):
    purchase_id: int = Query(..., description="row id of purchase table")
    status_to: str = Query(..., description="wanna status to this")


class NotiCanceledSchema(BaseModel):
    purchase_id: int = Query(..., description="row id of purchase table")


class UpdateAzooProductSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    azoo_product_row_id: int = Query(..., description="row id of azoo_product table")
    status_to: str = Query(..., description="wanna status to this")


class UploadateSamplingStatusSchema(BaseModel):
    target_table: str = Query(..., description="table name")
    row_id: int = Query(..., description="sampling table row id")
    status_to: str = Query(..., description="wanna status to this")
    mnt_path_in_nas: str | None = Query(..., description="zipfile path in the nas")


class FaqContentsSchema(BaseModel):
    id: int = Query(..., description="faq 목록 번호")
    category_id: int = Query(..., description="faq 컨텐츠가 속해있는 카테고리")
    status: int = Query(..., description="..")
    created_at: datetime = Query(..., description="..")
    updated_at: datetime = Query(..., description="faq 컨텐츠 최종수정일")
    is_deleted: bool = Query(..., description="...")
    question: str = Query(..., description="...")
    answer: str = Query(..., description="...")
    question_en: str = Query(..., description="...")
    answer_en: str = Query(..., description="...")

    class Config:
        from_attributes = True


class TaskDoneOutputSchema(BaseModel):
    dataset_score: str = Query(..., description="result dataset 의 accuracy 점수")
    # dataset_report_html_file_url:str|None = Query(default=None , description="result dataset 에 대한 report html을 다운로드 받을 수 있는 주소")
    output_path: str = Query(
        ..., description="report json 파일들이 들어있는 parent 디렉토리 path"
    )


class NotiTaskDoneSchema(BaseModel):
    task_id: UUID = Query(
        ...,
        description="task_id 값이다, 하지만 마이클이 ai모델의 결과zip 파일의 dataset_id 로도 사용한다. 이 값을 이용해서 download한다.",
    )
    begin_at: datetime = Query(..., description="task 시작시간")
    end_at: datetime = Query(..., description="task 종료시간")
    task_status: str = Query(
        ..., description="PENDING , RUNNING, CANCELED , SUCCESS , FAILED"
    )
    output: TaskDoneOutputSchema = Query(
        ...,
        description="result dataset 의 accuracy 점수와 report html 파일의 접근 주소",
    )


class CorePostSchema(BaseModel):
    """ """

    contract_name: str = Query(..., description="계약서 이름")
    common_message: str = Query(..., description="메시지")
    lang: str = Query(..., description="발송언어--   'ko' / 'eng' ")


class MakeContractSchema(BaseModel):
    contract_name: str = Query(..., description="계약서 이름")
    common_message: str = Query(..., description="메시지")
    lang: str = Query(..., description="발송언어--   'ko' / 'eng' ")
    user_name: str = Query(default="", description="user name")
    user_email: str | None = Query(default=None, description="user email")


class AddressCheckSchema(BaseModel):
    target_address: str = Query(..., description="회원가입을 원하는 고객의 주소지 정보")


class ReturnResultSchema(BaseModel):
    end_point: str = Query(..., description="API 엔드포인트")
    result: str = Query(..., description="API 의 반환메시지")
    response_date: datetime = Query(..., description="API의 결과값 반환시점")


class ReturnResultListSchema(BaseModel):
    end_point: str = Query(..., description="API 엔드포인트")
    result: list = Query(..., description="API 의 반환메시지")
    response_date: datetime = Query(..., description="API의 결과값 반환시점")


class ReturnResultDictSchema(BaseModel):
    end_point: str = Query(..., description="API 엔드포인트")
    result: dict = Query(..., description="API 의 반환메시지")
    response_date: datetime = Query(..., description="API의 결과값 반환시점")


class ReturnMessageSchema(BaseModel):
    end_point: str = Query(..., description="API 엔드포인트")
    msg: str = Query(..., description="API 의 반환메시지")
    response_date: datetime = Query(..., description="API의 결과값 반환시점")


DataT = TypeVar("DataT")  # Generic


class ReturnListDataSchema(BaseModel, Generic[DataT]):
    end_point: str = Query(..., description="API 엔드포인트")
    result: List[DataT] = Query(..., description="API 의 결과데이터")
    working_time_sec: int = Query(..., description="처리시간")
    response_date: datetime = Query(..., description="API의 메시지 반환시점")


class GenericPageSchema(BaseModel, Generic[DataT]):
    end_point: str = Query(..., description="API 엔드포인트")
    items: List[DataT] = Query(..., description="pagination items")
    length_of_items: int = Query(..., description="Count of items")
    total_table_items: int = Query(..., description="Count of total rows in the table")
    working_time_sec: int = Query(..., description="처리시간")
    response_date: datetime = Query(..., description="API의 메시지 반환시점")


##### 엘라스틱서치 자동완성 기능
class EsProductSchema(BaseModel):
    id: int
    name: str
    updated_at: datetime


class ReturnProductListSchema(BaseModel):
    posts_found: list[EsProductSchema]
