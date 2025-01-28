from sqlalchemy import (
    Column,
    ForeignKey,
    BIGINT,
)
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import relationship

from Settings.Database.database import engine

Base = automap_base()


class AdminModel(Base):
    __tablename__ = "admin"
    contracts = relationship("ContractModel", back_populates="admin", lazy="select")
    pass


class DatasetFormatModel(Base):
    __tablename__ = "m_product_data_format"
    product_registration_rows = relationship(
        "ProductRegistrationModel", back_populates="data_format", lazy="select"
    )
    pass


class DatasetTypeModel(Base):
    __tablename__ = "m_product_data_type"
    product_registration_rows = relationship(
        "ProductRegistrationModel", back_populates="data_type", lazy="select"
    )
    pass


class MContractStatusModel(Base):
    __tablename__ = "m_contract_status"
    contracts = relationship(
        "ContractModel", back_populates="contract_status", lazy="select"
    )
    pass


class ContractModel(Base):
    __tablename__ = "contract"
    product_registration_rows = relationship(
        "ProductRegistrationModel", back_populates="contract", lazy="select"
    )

    admin_id = Column(BIGINT, ForeignKey("admin.id"))
    # admin = relationship('AdminModel' , collection_class= set , back_populates = 'contracts'  )
    admin = relationship("AdminModel", back_populates="contracts", lazy="select")

    contract_status_id = Column(BIGINT, ForeignKey("m_contract_status.id"))
    contract_status = relationship(
        "MContractStatusModel", back_populates="contracts", lazy="select"
    )
    pass


class ProductRegistrationModel(Base):
    __tablename__ = "product_registration"
    synthetic_tasks = relationship(
        "GeneratingSyntheticTasksModel",
        back_populates="product_registration_row",
        lazy="select",
    )

    contract_id = Column(BIGINT, ForeignKey("contract.id"))
    contract = relationship(
        "ContractModel", back_populates="product_registration_rows", lazy="select"
    )

    data_format_id = Column(BIGINT, ForeignKey("m_product_data_format.id"))
    data_format = relationship(
        "DatasetFormatModel", back_populates="product_registration_rows", lazy="select"
    )

    data_type_id = Column(BIGINT, ForeignKey("m_product_data_type.id"))
    data_type = relationship(
        "DatasetTypeModel", back_populates="product_registration_rows", lazy="select"
    )

    upload_status_id = Column(BIGINT, ForeignKey("m_upload_status.id"))
    upload_status = relationship(
        "UploadStatusModel", back_populates="product_registration_rows", lazy="select"
    )

    azoo_product = relationship(
        "AzooProductModel", back_populates="product_registration_row", lazy="select"
    )
    sampling_rows = relationship(
        "SamplingModel", back_populates="product_registration_row", lazy="select"
    )
    # sampling_rows = relationship('SamplingModel' , back_populates='product_registration_row' , lazy='joined', foreign_keys='SamplingModel.product_registration_id')
    pass


class UploadStatusModel(Base):
    __tablename__ = "m_upload_status"
    product_registration_rows = relationship(
        "ProductRegistrationModel", back_populates="upload_status", lazy="select"
    )
    pass


class GeneratingSyntheticTasksModel(Base):
    __tablename__ = "genarating_synthetic_tasks"
    product_registration_id = Column(BIGINT, ForeignKey("product_registration.id"))
    product_registration_row = relationship(
        "ProductRegistrationModel", back_populates="synthetic_tasks", lazy="select"
    )

    sampling_row = relationship(
        "SamplingModel", back_populates="synthetic_task_row", lazy="select"
    )
    pass


class AzooProductModel(Base):
    __tablename__ = "azoo_product"
    product_registration_row = relationship(
        "ProductRegistrationModel", back_populates="azoo_product", lazy="select"
    )
    product_warehouse_rows = relationship(
        "ProductWarehouseModel", back_populates="azoo_product", lazy="select"
    )
    pass


class MSamplingStatusModel(Base):
    __tablename__ = "m_sampling_status"
    sampling_rows = relationship(
        "SamplingModel", back_populates="sampling_status", lazy="select"
    )
    pass


class SamplingModel(Base):
    __tablename__ = "sampling"
    status_id = Column(BIGINT, ForeignKey("m_sampling_status.id"))
    sampling_status = relationship(
        "MSamplingStatusModel", back_populates="sampling_rows", lazy="select"
    )

    product_registration_id = Column(BIGINT, ForeignKey("product_registration.id"))
    product_registration_row = relationship(
        "ProductRegistrationModel", back_populates="sampling_rows", lazy="select"
    )
    # product_registration_row = relationship('ProductRegistrationModel' , back_populates='sampling_rows' , lazy='joined', foreign_keys=[product_registration_id])

    synthetic_task_row = relationship(
        "GeneratingSyntheticTasksModel", back_populates="sampling_row", lazy="select"
    )
    product_warehouse_row = relationship(
        "ProductWarehouseModel", back_populates="sampling_row", lazy="select"
    )
    pass


class ProductWarehouseModel(Base):
    __tablename__ = "product_warehouse"
    product_id = Column(BIGINT, ForeignKey("azoo_product.id"))
    azoo_product = relationship(
        "AzooProductModel", back_populates="product_warehouse_rows", lazy="select"
    )

    sampling_id = Column(BIGINT, ForeignKey("sampling.id"))
    sampling_row = relationship(
        "SamplingModel", back_populates="product_warehouse_row", lazy="select"
    )
    # sampling_row = relationship('SamplingModel' , back_populates='product_warehouse_row' , lazy='joined', foreign_keys=[sampling_id])
    pass


Base.prepare(autoload_with=engine)  ### automap_base 를 쓸때, 사용하는거야.
PurchaseModel = Base.classes.purchase
MPurchaseStatusModel = Base.classes.m_purchase_status
MgeneratingSyntheticTaskStatus = Base.classes.m_genarating_synthetic_task_status
MProductWarehouseStatusModel = Base.classes.m_product_warehouse
MAzooProductStatusModel = Base.classes.m_azoo_product_status
ProjectModel = Base.classes.projects
UserModel = Base.classes.user
