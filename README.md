```shell
# sanic http server
python server.py

# celery monitor - flower
celery -A tasks flower

# celery worker
celery -A tasks worker -l info
```