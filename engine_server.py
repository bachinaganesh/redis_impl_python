from redis import Redis
import json
from multiprocessing import Pool
import time
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add handler to output logs to the console
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


r = Redis(host="localhost", port=6379, decode_responses=True)

def worker(task_json: str):
    task = json.loads(task_json)
    task_id = task["id"]
    payload = task["payload"]
    try:
        process_task(task_id, payload)
    except Exception as e:
        r.hset(f"{task:{task_id}}", mapping={"status": "failed", "progress": "100"})


def process_task(task_id, payload):
    print(f"Payload start for task : {task_id}")

    # Task1: fetching lookuptask is processing
    logger.info(f"Fetching lookup data and task id: {task_id}")
    time.sleep(5)
    # when task1 is completed here update the prgress to 15 percentage
    publish_progress(task_id, "processing", "20")

    # Task2: Processing strategy 
    logger.info(f"Processing strategy resuly and task id: {task_id}")
    time.sleep(10)
    # when task2 is completed here update the progress to 50 percentage
    publish_progress(task_id, "processing", "75")

    # Task3: Store the data to database
    logger.info(f"Store the data into database and task id: {task_id}")
    time.sleep(5)
    # when task3 is completed update the staus and progress is 100 
    publish_progress(task_id, "completed", "100", "Task is completed")
    # send the json response 


def publish_progress(task_id, status, progress, result = None):
    # send the progress back to UI based in socket connection
    task_update = {
        "task_id": task_id,
        "status": status,
        "progress": progress
    }

    if result:
        task_update["result"] = result

    r.hset(f"task:{task_id}", mapping=task_update)
    r.publish(f"task_update:{task_id}", json.dumps(task_update))



def start_engine():
    pool = Pool(processes=5)
    try:
        while True:
            _, task_josn = r.brpop("task_queue")
            if not task_josn:
                continue
            pool.apply_async(worker, args=(task_josn, ))
    finally:
        pool.close()
        pool.join()



if __name__ == "__main__":
    start_engine()
