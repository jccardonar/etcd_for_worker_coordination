import argparse
import asyncio
import configparser
import logging
import os
import sys

import etcd3
from common_libraries import EtcdConfig
from worker import Worker, WorkerConfiguration

# Read Arguments
# =====
parser = argparse.ArgumentParser()
parser.add_argument(
    '-c',
    '--config',
    help='Config file',
    type=str,
    required=False,
    default="config.ini")

args = parser.parse_args()

config_file = args.config
if not os.path.isfile(config_file):
    raise Exception("%s is not a valid file.", config_file)

# Parse configuration
# ====
config = configparser.ConfigParser()
config.read_file(open(config_file))

# Replace this for an actual logging file if needed
try:
    logging_file = config["logging"]["file"]
except:
    logging_file = "worker.log"

etcd_ip = config['etcd']['etcd_ip']
etcd_port = int(config['etcd']['etcd_port'])
group_prefix = config['etcd']['group_prefix']
workers_subprefix = config['etcd']['workers_subprefix']
worker_time_to_renew = int(config['worker']['time_to_renew'])
worker_queue_subprefix = config['etcd']['worker_queue_subprefix']

# Prepare logging
# ===
# Use a logging config file in a production env.
logger = logging.getLogger(__name__)
handlers = set()

# stream handler
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s %(asctime)s %(levelname)s %(message)s')
stream_handler.setFormatter(formatter)
handlers.add(stream_handler)

# if there is a folder in the environment for logging, use it
file_handler = logging.FileHandler(logging_file)
file_handler.setLevel(logging.DEBUG)
handlers.add(file_handler)

logger.level = logging.DEBUG
if not logger.handlers:
    for handler in handlers:
        logger.addHandler(handler)

# Prepare Loop
# ====
loop = asyncio.get_event_loop()

# Create worker
# ====
uuid = "1"
result_file = "results"
worker_configuration = WorkerConfiguration(
    uuid, group_prefix, workers_subprefix, worker_queue_subprefix,
    worker_time_to_renew, result_file)

etcd_config = EtcdConfig(etcd_ip, etcd_port)

worker = Worker(worker_configuration, etcd_config, loop, logger)


def operate_task_to_worker(etcd, worker_queue, operation, task_id):
    key = worker_queue + "." + str(task_id)
    if operation == "delete":
        etcd.delete(key)
    elif operation == "put":
        etcd.put(key, task_id)
    logger.info("%s task %s", operation, task_id)


# define worker controller
async def control_worker(uuid):
    etcd = etcd3.client(host=etcd_ip, port=etcd_port)
    # wait a second for worker to start
    await asyncio.sleep(1)
    worker_queue = '.'.join([group_prefix, worker_queue_subprefix, uuid])
    # create a task
    operate_task_to_worker(etcd, worker_queue, "put", "1")
    await asyncio.sleep(10)
    # create the same task again
    operate_task_to_worker(etcd, worker_queue, "put", "1")
    await asyncio.sleep(10)
    # create a second task
    operate_task_to_worker(etcd, worker_queue, "put", "2")
    await asyncio.sleep(10)
    # delete second task
    operate_task_to_worker(etcd, worker_queue, "delete", "2")
    await asyncio.sleep(10)
    # create third task
    operate_task_to_worker(etcd, worker_queue, "put", "3")
    await asyncio.sleep(10)
    # delete all tasks
    etcd.delete_prefix(worker_queue)
    await asyncio.sleep(10)
    worker.cancel()


# delete current prefixes and results file
etcd = etcd3.client(host=etcd_ip, port=etcd_port)
etcd.delete_prefix(group_prefix)
with open(result_file, 'w'):
    pass

tasks = asyncio.gather(worker.run(), control_worker(uuid))

try:
    loop.run_until_complete(tasks)
except KeyboardInterrupt as e:
    pass
except:
    raise
finally:
    worker.cancel()
