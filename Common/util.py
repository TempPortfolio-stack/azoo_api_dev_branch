import concurrent.futures
import datetime
import os
import smtplib
import socket
import traceback
import uuid
import zlib
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests
import uuid_utils

from Settings.Env.env_settings import settings
from Settings.Logger.logging_config import util_logger as logger

SECRET_KEY = settings.get("SECRET_KEY")


async def send_email_of_ready_premium_product(target_email_addr):
    try:
        email_template = f"""
                <table
                width="95%"
                border="0"
                align="center"
                cellpadding="0"
                cellspacing="0"
                style="
                    max-width: 670px;
                    background: #fff;
                    border-radius: 3px;
                    text-align: center;
                    -webkit-box-shadow: 0 6px 18px 0 rgba(0, 0, 0, 0.06);
                    -moz-box-shadow: 0 6px 18px 0 rgba(0, 0, 0, 0.06);
                    box-shadow: 0 6px 18px 0 rgba(0, 0, 0, 0.06);
                ">
                <tr>
                    <td style="height: 40px">&nbsp;</td>
                </tr>
                <tr>
                    <td style="padding: 0 35px">
                    <h1
                        style="
                        color: #1e1e2d;
                        font-weight: 500;
                        margin: 0;
                        font-size: 32px;
                        font-family: 'Rubik', sans-serif;
                        ">
                        Your Premium Dataset is Ready!
                    </h1>
                    <span
                        style="
                        display: inline-block;
                        vertical-align: middle;
                        margin: 29px 0 26px;
                        border-bottom: 1px solid #cecece;
                        width: 100px;
                        "></span>
                    <p style="color: #455056; font-size: 15px; line-height: 24px; margin: 0">
                        Premium Dataset's generation task is completed. <br/>
                        You can download your premium dataset in your purchase history page. <br/>
                    </p>
                    <a
                        href="https://azoo.ai"
                        style="
                        cursor: pointer;
                        background: #20e277;
                        text-decoration: none !important;
                        font-weight: 500;
                        margin-top: 35px;
                        color: #fff;
                        text-transform: uppercase;
                        font-size: 14px;
                        padding: 10px 24px;
                        display: inline-block;
                        border-radius: 50px;
                        ">
                        Go to the AZOO
                    </a>
                    </td>
                </tr>
                <tr>
                    <td style="height: 40px">&nbsp;</td>
                </tr>
                </table>
                """

        # 이메일 설정
        sendEmail = settings["sendEmail"]
        # recvEmail = "kahrizma@naver.com"
        password = settings["email_password"]
        smtpName = settings["smtpName"]
        smtpPort = int(settings["smtpPort"])

        # 이메일 내용 설정
        subject = "[Azoo] Premium Product is ready"

        # MIME 멀티파트 객체 생성
        msg = MIMEMultipart()
        msg["From"] = sendEmail
        msg["To"] = target_email_addr
        msg["Subject"] = subject

        # HTML 본문 추가
        msg.attach(MIMEText(email_template, "html"))

        # 이메일 서버 연결 및 이메일 전송
        s = smtplib.SMTP(smtpName, smtpPort)
        s.starttls()  # TLS 보안 처리
        s.login(sendEmail, password)
        s.sendmail(sendEmail, target_email_addr, msg.as_string())
        s.close()

        logger.info(f"Email sent successfully! -- user_email:{target_email_addr}")
        pass
    except:
        logger.error(f"user_email:{target_email_addr} 로 이메일 전송 실패!!")
        logger.error(traceback.format_exc())
        raise
    pass


def get_azoo_fastapi_access_token():
    try:
        res = requests.post(
            url=f"{settings['HOST_ADDR_PREFIX']}/azoo_fastapi/auth/login",
            json={"user": settings["ALLOWED_USER"]},
        )

        if res.status_code == 200:
            result = res.json()
            result = result["access_token"]
            logger.info(f"Access Token을 발급받았습니다. :: {result}")
        else:
            result = None
            logger.info("Access Token을 발급에 실패했습니다.")
        return result
    except:
        logger.info("Access Token을 발급에 실패했습니다.")
        logger.error(traceback.format_exc())
        return None


def generate_uuid(param_info: str):
    try:
        try:
            server_info = socket.gethostbyname(socket.gethostname())
        except:
            server_info = "127.0.0.1"
            pass
        return str(
            uuid.uuid5(
                namespace=uuid.NAMESPACE_DNS,
                name=f"{os.getpid()}_{str(datetime.now())}_{param_info}_{server_info}_{str(uuid.uuid4())}",
            )
        )
    except:
        raise Exception("uuid 생성중 오류 발생!")


def generate_uuid7():
    try:
        return uuid_utils.uuid7()
    except:
        raise Exception("uuid7 생성중 오류 발생!")


def compress_string(s: str) -> bytes:
    try:
        return zlib.compress(s.encode("utf-8"))
    except:
        raise Exception("zlib으로 압축 하는 중 오류 발생!")


def decompress_string(b: bytes) -> str:
    try:
        return zlib.decompress(b).decode("utf-8")
    except:
        raise Exception("zlib에서 압축 푸는 중 오류 발생!")


def api_post_call(data: dict):
    try:
        retry_cnt = 3
        for i in range(retry_cnt):
            res = requests.post(
                url=data["url"], json=data["req_info"], headers=data["headers"]
            )
            if res.status_code == 200:
                break
            else:
                logger.warning(
                    f"data:{data} 의 호출에 실패했습니다. ------> retry_cnt: {i+1}/{retry_cnt}"
                )
        return res
    except:
        raise


def call_api(headers=None, method=None, url=None, req_info=None):
    try:
        data = {"req_info": req_info, "headers": headers, "url": url}

        if method.lower() == "post":
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(api_post_call, data)
                result = future.result()
            return result
        pass
    except:
        logger.error(traceback.format_exc())
        pass
    pass


def call_azoo_api(method=None, url=None, req_info=None):
    try:
        access_token = get_azoo_fastapi_access_token()
        if access_token is None:
            raise Exception("현재, AZOO Fastapi의 Access Token을 발급받을 수 없습니다.")

        headers = {"Authorization": f"Bearer {access_token}"}
        data = {"req_info": req_info, "headers": headers, "url": url}

        if method.lower() == "post":
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(api_post_call, data)
                result = future.result()
            return result
        pass
    except:
        logger.error(traceback.format_exc())
        logger.error(f"Access 토큰 발급받지 못해서, 에러 윗 단계로 raise")
        raise


# async def get_access_token_by_refresh():
#     try:
#         refresh_token = settings['REFRESH_TOKEN']
#         url = settings['REFRESH_TOKEN_URL']
#         data = {
#             "refresh_token": refresh_token
#         }

#         response = requests.post(url , json=data)
#         logger.info(f"Status Code : {response.status_code}")
#         if response.status_code == 200:
#             result = response.json()
#             logger.info(f"JSON Response : {json.dumps(result , indent=4 , ensure_ascii=False)}")
#             access_token = result['data']['access_token']
#             _ = result['data']['refresh_token']
#             token_type = result['data']['token_type']
#         else:
#             raise Exception("access토큰 refresh 중 에러 발생")
#         return {"access_token":access_token , "token_type":token_type}
#     except:
#         logger.error(traceback.format_exc())
#         return None


# async def get_new_access_token():
#     try:
#         url = settings['NEW_TOKEN_URL']
#         data = {
#         "client_id": "Y7zzYAzAnvCaXZY",
#         "client_secret": "vaT7soDazay7i5r"
#         }

#         response = requests.post(url , json=data)
#         logger(f"Status Code : {response.status_code}")
#         if response.status_code == 200:
#             result = response.json()
#             logger.info(f"JSON Response : {json.dumps(result , indent=4 , ensure_ascii=False)}")
#             access_token = result['data']['access_token']     # 유효기간 1시간
#             _ = result['data']['refresh_token']   # 100일동안 동일 --> 100일 지나면, 아예 새로 토큰을 발급 받아야 함
#             token_type = result['data']['token_type']
#         else:
#             raise Exception("access토큰 새로 발행중 에러 발생")
#         return {"access_token":access_token , "token_type":token_type}
#     except:
#         logger.error(traceback.format_exc())
#         raise
