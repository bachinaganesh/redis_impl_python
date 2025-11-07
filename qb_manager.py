from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis
import uuid
import json
import asyncio

app = FastAPI()
redis_async = aioredis.Redis(host="localhost", port=6379, decode_responses=True)

async def enqueue_task(payload: dict):
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "payload": payload,
        "status": "queued"
    }
    await redis_async.lpush("task_queue", json.dumps(task))
    await redis_async.hset(f"task:{task_id}", mapping={"status": "queued", "progress": "0"})
    return task_id

@app.post("/manager/receive-legs")
async def submit_task(request: Request):
    payload = await request.json()
    task_id = await enqueue_task(payload)
    return JSONResponse({
        "task_id": task_id,
        "status": "queued"
    }, 
    status_code=200)

@app.get("/status/{task_id}")
async def task_status(task_id):
    task_key = f"task:{task_id}"
    task_data = await redis_async.hgetall(task_key)
    if not task_data:
        return JSONResponse({
            "error": "Task not found with an id: "+task_id
        }, 
        status_code=404)
    else:
        return JSONResponse({
            "task_id": task_id,
            "status": task_data.get("status", "Queue"),
            "progress": task_data.get("progress", "0")
        },
        status_code=200)
    

@app.websocket("/ws/{task_id}")
async def websocket_endpoint(websocket: WebSocket, task_id: str):
    await websocket.accept()
    try:
        pub = redis_async.pubsub()
        await pub.subscribe(f"task_update:{task_id}")
        # if connection fails then get the update from redis_hash instaed of redis pub/sub
        task_data = await redis_async.hgetall(f"task:{task_id}")
        task_data["task_id"] = task_id
        if task_data:
            await websocket.send_text(json.dumps(task_data))
        while True:
            message = await pub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                await websocket.send_text(message["data"])
            await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        print(f"WebSocket disconnected for task {task_id}")
