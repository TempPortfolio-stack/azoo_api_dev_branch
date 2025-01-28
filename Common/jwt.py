from datetime import datetime, timedelta

from fastapi import Request, HTTPException
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt

from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import fastapi_logger


class JWTBearer(HTTPBearer):
    secret_key = settings["JWT_SECRET"]
    jwt_algorithm = settings["JWT_ALGORITHM"]
    allowed_user = settings["ALLOWED_USER"]

    def __init__(self, auto_error: bool = False):
        super(JWTBearer, self).__init__(auto_error=auto_error)

    async def __call__(self, request: Request):
        credentials: HTTPAuthorizationCredentials = await super(
            JWTBearer, self
        ).__call__(request)
        if credentials:
            if not credentials.scheme == "Bearer":
                raise HTTPException(
                    status_code=401, detail="Invalid authentication scheme."
                )
            if not self.verify_jwt(credentials.credentials):
                raise HTTPException(
                    status_code=401, detail="Invalid token or expired token."
                )
            return credentials.credentials
        else:
            raise HTTPException(status_code=401, detail="Invalid authorization code.")

    def verify_jwt(self, jwt_token: str) -> bool | None:
        try:
            payload = self.decode_jwt(jwt_token)
            exp_of_token = payload["exp"]

            fastapi_logger.info(f"exp:: {exp_of_token}, {type(exp_of_token)}")
            if (
                datetime.now().timestamp() > exp_of_token
            ):  ## datetime 을 unix 타임으로 변환
                fastapi_logger.info("만료기간 테스트 --불통")
                return False
            fastapi_logger.info("만료기간 테스트 --통과")

            if payload["user"] != settings["ALLOWED_USER"]:
                fastapi_logger.info("사용자 이름 테스트 --불통")
                return False
            fastapi_logger.info("사용자 이름 테스트 --통과")
            return True
        except:
            raise HTTPException(
                status_code=401, detail="Invalid token or expired token."
            )

    def decode_jwt(self, access_token: str) -> dict | None:
        try:
            payload: dict = jwt.decode(
                access_token, JWTBearer.secret_key, algorithms=[JWTBearer.jwt_algorithm]
            )
            return payload
        except:
            return None

    @classmethod
    def create_jwt(cls, username: str) -> str | None:
        try:
            exp = datetime.now() + timedelta(days=1)
            payload = {
                "user": username,  # unique id
                "exp": exp,  # 이렇게하면,  JWT (JSON Web Tokens) 표준에 의해, unix timestamp 로 바뀐다.
            }
            return jwt.encode(payload, key=cls.secret_key, algorithm=cls.jwt_algorithm)
        except:
            return None
