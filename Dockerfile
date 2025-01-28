FROM python:3.12.3-slim AS builder

WORKDIR /app

COPY requirements.txt ./

RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

FROM python:3.12.3-slim
ENV PYTHONUNBUFFERED 1
WORKDIR /app

# 빌드 스테이지에서 설치한 패키지만 최종 이미지에 복사
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY . .

# 로그 디렉토리와 파일 생성 및 권한 설정
# RUN mkdir -p /app/Logs && \
#     touch /app/Logs/access_log /app/Logs/error_log

# RUN chmod -R 777 /app/Logs && \
#     chmod 666 /app/Logs/access_log /app/Logs/error_log

RUN mkdir -p /app/Logs 
RUN chmod -R 777 /app/Logs

EXPOSE 9090

# 타임존 환경 변수 설정
ENV TZ=Asia/Seoul
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# 애플리케이션 관련 환경 변수 설정
ENV GUNICORN_ACCESS_LOG=/app/Logs/access_log \
    GUNICORN_ERROR_LOG=/app/Logs/error_log

CMD ["gunicorn", "-c" , "/app/Settings/Deploy/gunicorn_conf.py" , "main:app"]
