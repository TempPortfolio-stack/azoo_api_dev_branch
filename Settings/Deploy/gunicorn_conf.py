import os

bind = "0.0.0.0:9090"

workers = 10
worker_class = "uvicorn.workers.UvicornWorker"

timeout = 3600

accesslog = os.getenv("GUNICORN_ACCESS_LOG")
errorlog = os.getenv("GUNICORN_ERROR_LOG")
