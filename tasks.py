import time
import random
import logging
from celery.utils.log import get_task_logger
from error import LogErrorsTask

from celery import Celery

app = Celery("tasks")
app.config_from_object("celeryconfig")


# logger = get_task_logger(__name__)
# logger.setLevel(logging.INFO)

@app.task
def add(x, y):
    return x + y


@app.task
def multi(*args):
    res = 1
    for x in args:
        res = res * x
    return res


@app.task
def xmulti(args):
    print(args)
    res = 1
    for t in args:
        for x in t:
            res = res * x
    return res


@app.task
def sum_array(num_array):
    count = 0
    for i in num_array:
        count += i
    return count


@app.task
def add_rand_sleep(x, y):
    if x > 3:
        raise ValueError("x 大于 3")
    time.sleep(random.randint(0, 5))
    return x + y


@app.task
def tap(x):
    return x


@app.task
def done(x="no_args"):
    print(x)
    print("done")


if __name__ == '__main__':
    app.start()
