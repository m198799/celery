from tasks import add_rand_sleep, app as capp

from sanic import Sanic
from sanic.response import json

app = Sanic("My Hello, world app")


@app.get("/sync")
async def start_task(request):
    r = add_rand_sleep.delay(1, 2)
    return json({'res': r.get()})


@app.get("/async")
async def start_task(request):
    r = add_rand_sleep.delay(1, 2)
    return json({'res': r.id})


@app.get("/task/<id>")
async def start_task(request, id):
    r = capp.AsyncResult(id)
    return json({'res': r.get()})


if __name__ == '__main__':
    app.run()
