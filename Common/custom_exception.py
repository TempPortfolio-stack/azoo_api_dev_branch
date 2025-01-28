from fastapi import Request, status
from fastapi.responses import JSONResponse
from naver_logger import LogClient

log_client = LogClient(url="http://elsa-col.ncloud.com/_store")


class UnicornException(Exception):
    def __init__(self, name: str, error_code: int = 400):
        self.name = name
        self.error_code = error_code


async def unicorn_exception_handler(request: Request, exc: UnicornException):
    log_client.send_log(
        {
            "request_method": request.method,
            "request_url": str(request.url),
            "request_headers": dict(request.headers),
            "error_code": exc.error_code,
            "exception_message": exc.name,
        },
        level="ERROR",
    )
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"message": f"{exc.name}", "error_code": exc.error_code},
    )


# Define custom error codes and descriptions
ERROR_DESCRIPTIONS = {
    1: "You have unfinished task. Please try again later.",
    2: "User already activated by invitation code.",
    3: "Reward action not found.",
    4: "Reward not enough.",
    5: "Invitation code had been used.",
}


class JwtException(Exception):
    def __init__(self, err_msg: str):
        self.err_msg = err_msg
        pass

    def __str__(self) -> str:
        return self.err_msg

    pass
