from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from redis import Redis
import uuid
import json

app = FastAPI()
r = Redis(decode_responses=True)

def enqueue_task(payload: dict):
    task_id = str(uuid.uuid4())
    task = {
        "id": task_id,
        "payload": payload,
        "status": "queued"
    }
    r.lpush("task_queue", json.dumps(task))
    r.hset(f"task:{task_id}", mapping={"status": "queued"})
    return task_id

@app.post("/submit-task")
async def submit_task(request: Request):
    payload = await request.json()
    task_id = enqueue_task(payload)
    return {
        "task_id": task_id,
        "status": "queued"
    }