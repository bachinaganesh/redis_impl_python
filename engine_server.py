from redis import Redis
import json
from concurrent.futures import ThreadPoolExecutor
import time

r = Redis(decode_responses=True)
pool = ThreadPoolExecutor(max_workers=5)

def worker(task_json: str):
    task = json.loads(task_json)
    task_id = task["id"]
    payload = task["payload"]
    r.hset(f"task:{task_id}", mapping={"status": "processing"})
    process_task(task_id, payload)


def process_task(task_id, payload):
    print(f"Payload start for task : {task_id}")
    time.sleep(50)
    print(f"Payload completed for task : {task_id}")

def start_engine():
    while True:
        _, task_josn = r.brpop("task_queue")
        pool.submit(worker, task_josn)


if __name__ == "__main__":
    start_engine()
