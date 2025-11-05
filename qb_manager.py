from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from redis import asyncio as aioredis
import uuid
import json

app = FastAPI()
r = aioredis.Redis(host="localhost", port=6379, decode_responses=True)

async def enqueue_task(payload: dict):
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "payload": payload,
        "status": "queued"
    }
    await r.lpush("task_queue", json.dumps(task))
    await r.hset(f"task:{task_id}", mapping={"status": "queued", "progress": "0"})
    return task_id

@app.post("/submit-task")
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
    task_data = await r.hgetall(task_key)
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