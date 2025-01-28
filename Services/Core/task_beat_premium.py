# celery -A Settings.Worker.celery_worker beat   # 이렇게 beat 를 실행시켜줘야 한다. 이게 task를 발행하는 역할을 한다.
# celery -A Settings.Worker.celery_worker worker -Q celery_periodic_queue -n beat_contract --concurrency=1

from Settings.Worker.celery_worker import celery_app


@celery_app.task(bind=True)
def task_premium_product_update(self):
    try:
        pass
    except:
        pass
