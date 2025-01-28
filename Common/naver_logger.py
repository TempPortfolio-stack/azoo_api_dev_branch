import json

import requests

from Settings.Env.env_settings import settings


####  네이버 클라우드 로거 사용법
# https://guide.ncloud-docs.com/docs/elsa-elsa-1-5-1

# $ curl -XPOST 'https://elsa-col.ncloud.com/_store' -d '{
#     "projectName": "%YOUR_PROJECT_ID%",
#     "projectVersion": "1.0.0",
#     "body": "this log message come from https client, and it is a simple sample.",
# 	"logLevel": "DEBUG",
# 	"logType": "WEB",
# 	"logSource": "https"
# }'


class LogClient:
    def __init__(self, url):
        self.url = url

    def send_log(self, body: dict, level="INFO"):
        try:
            headers = {"Content-type": "application/json"}
            body_request = {
                "projectName": settings["NAVERCLOUD_PROJECT_ID"],
                "projectVersion": "1.0.0",
                "body": json.dumps(body),
                "logLevel": level,
                "logType": "WEB",
                "logSource": "https",
            }

            requests.post(self.url, data=json.dumps(body_request), headers=headers)
        except Exception as e:
            print(e)


log_client = LogClient(url="http://elsa-col.ncloud.com/_store")
