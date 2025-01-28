from sqlalchemy import create_engine, URL
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession

# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker

from Settings.Env.env_settings import settings

db_url = settings.get("DB_SERVER")
db_port = settings.get("DB_PORT")
db_name = settings.get("DB_NAME")
db_user = settings.get("DB_USER")
db_passwd = settings.get("DB_PASSWD")


url_object = URL.create(
    "postgresql+psycopg2",
    username=db_user,
    password=db_passwd,  # plain (unescaped) text
    host=db_url,
    port=db_port,
    database=db_name,
)


async_url_object = URL.create(
    "postgresql+asyncpg",
    username=db_user,
    password=db_passwd,  # plain (unescaped) text
    host=db_url,
    port=db_port,
    database=db_name,
)


engine = create_engine(url_object)
# async_engine = create_async_engine(async_url_object , pool_size=20, max_overflow=10)
async_engine = create_async_engine(
    async_url_object,
    pool_pre_ping=True,
    pool_recycle=300,
    pool_size=100,
    max_overflow=200,
)

# Base = declarative_base()

# reflect the tables
# Base = automap_base()
# Base.prepare(autoload_with=dev_engine)


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
AsyncSessionLocal = sessionmaker(
    async_engine, expire_on_commit=False, class_=AsyncSession
)  # 비동기 세션


def get_db():
    # db = SessionLocal_service()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def async_get_db():
    async with AsyncSessionLocal() as db:
        yield db
