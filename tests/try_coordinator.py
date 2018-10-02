import argparse
import asyncio
import configparser
import logging
import os
import sys

import etcd3
from common_libraries import EtcdConfig
from coordinator import Coordinator, CoordinatorConfiguration

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
global_task_prefix = config['etcd']['global_task_prefix']
group_prefix = config['etcd']['group_prefix']
workers_subprefix = config['etcd']['workers_subprefix']
coordinator_subprefix = config["etcd"]["coordinator_subprefix"]
coordinator_time_to_renew = int(config['coordinator']['time_to_renew'])
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

# Create agents
# ====
# coordinator agents are the entities that can become coordinators.
etcd_config = EtcdConfig(etcd_ip, etcd_port)

# coordinator agent 1
uuid = "1"
coordinator_configuration = CoordinatorConfiguration(
    uuid, group_prefix, workers_subprefix, global_task_prefix,
    coordinator_subprefix, worker_queue_subprefix, coordinator_time_to_renew)
agent_one = Coordinator(coordinator_configuration, etcd_config, loop, logger)

# coordinator agent 2
uuid = "2"
coordinator_configuration = CoordinatorConfiguration(
    uuid, group_prefix, workers_subprefix, global_task_prefix,
    coordinator_subprefix, worker_queue_subprefix, coordinator_time_to_renew)
agent_two = Coordinator(coordinator_configuration, etcd_config, loop, logger)


def operation_on_task(etcd, task_id, operation):
    key = global_task_prefix + "." + task_id
    if operation == "delete":
        etcd.delete(key)
    elif operation == "put":
        etcd.put(key, task_id)
    logger.info("%s task %s", operation, task_id)


def operation_on_worker(etcd, worker_id, operation):
    key = group_prefix + "." + workers_subprefix + "." + worker_id
    if operation == "delete":
        etcd.delete(key)
    elif operation == "put":
        etcd.put(key, worker_id)
    logger.info("%s worker  %s", operation, worker_id)


# define worker controller
async def control_agents(agents):
    etcd = etcd3.client(host=etcd_ip, port=etcd_port)
    # wait a second for coordinator to start
    await asyncio.sleep(2)
    # find out if any agent is coordinator
    current_coordinator = None
    for coordinator in agents:
        if coordinator.am_I_coordinator():
            current_coordinator = coordinator
            break
    if current_coordinator is None:
        raise Exception("No coordinator found")

    # register a worker
    workerid = "1w"
    operation_on_worker(etcd, workerid, "put")
    await asyncio.sleep(10)
    # Check that both coordinator have the worker
    for coordinator in agents:
        if workerid not in coordinator.workers:
            raise Exception(
                f"coordinator {coordinator.uuid} does not have worker {workerid}"
            )
    # register a task
    taskid = "1"
    operation_on_task(etcd, taskid, "put")
    await asyncio.sleep(10)
    # Check that both coordinator have the task, and that it is assigned to a worker
    for coordinator in agents:
        if taskid not in coordinator.task_list:
            raise Exception(
                f"coordinator {coordinator.uuid} does not have worker {taskid}"
            )
        if taskid not in set(
                task for tasks in coordinator.tasks_per_worker.values()
                for task in tasks):
            raise Exception(
                f"coordinator {coordinator.uuid} does not have worker {taskid} assigned to any worker"
            )

    # register another task
    taskid = "2"
    operation_on_task(etcd, taskid, "put")
    await asyncio.sleep(10)
    # check that both have the task
    for coordinator in agents:
        if taskid not in coordinator.task_list:
            raise Exception(
                f"coordinator {coordinator.uuid} does not have worker {taskid}"
            )
        if taskid not in set(
                task for tasks in coordinator.tasks_per_worker.values()
                for task in tasks):
            raise Exception(
                f"coordinator {coordinator.uuid} does not have worker {taskid} assigned to any worker"
            )

    # register another worker
    operation_on_worker(etcd, "2w", "put")
    await asyncio.sleep(10)
    # delete first worker
    operation_on_worker(etcd, "1w", "delete")
    await asyncio.sleep(10)

    # Check that all tasks are currently served and that only one worker remains
    for coordinator in agents:
        all_tasks = set(task
                        for tasks in coordinator.tasks_per_worker.values()
                        for task in tasks)
        if all_tasks != set(["1", "2"]):
            raise Exception(
                f"coordinator {coordinator.uuid} does not server all tasks, only {all_tasks}"
            )
        if coordinator.workers != set(["2w"]):
            raise Exception(
                f"coordinator {coordinator.uuid} does not have the right number of workers"
            )

    # delete a coordinator
    current_coordinator.cancel()
    logger.info("Canceling the coordinator")
    await asyncio.sleep(20)
    # checking that there is still a coordinator
    older_coordinator = current_coordinator
    current_coordinator = None
    for coordinator in agents:
        if coordinator.am_I_coordinator():
            current_coordinator = coordinator
            break
    if current_coordinator == older_coordinator:
        raise Exception("Still same coordinator")
    if current_coordinator is None:
        raise Exception("No coordinator found")

    # create another worker
    operation_on_worker(etcd, "1w", "put")
    await asyncio.sleep(10)
    # create another task
    operation_on_task(etcd, "3", "put")
    await asyncio.sleep(10)
    # create another task
    operation_on_task(etcd, "4", "put")
    await asyncio.sleep(10)
    # delete a task
    operation_on_task(etcd, "2", "delete")
    await asyncio.sleep(10)

    # ensure that all tasks are assigned
    all_tasks = set(task
                    for tasks in current_coordinator.tasks_per_worker.values()
                    for task in tasks)
    if all_tasks != set(["1", "3", "4"]):
        raise Exception(
            f"coordinator {current_coordinator} does not have the right tasks. It has {all_tasks}"
        )

    for coordinator in agents:
        coordinator.cancel()


# delete current prefixes and results file
etcd = etcd3.client(host=etcd_ip, port=etcd_port)
etcd.delete_prefix(group_prefix)
etcd.delete_prefix(global_task_prefix)

agents = [agent_one, agent_two]

tasks = asyncio.gather(agent_one.run(), agent_two.run(),
                       control_agents(agents))

try:
    loop.run_until_complete(tasks)
except KeyboardInterrupt as e:
    pass
except:
    raise
finally:
    for coordinator in agents:
        coordinator.cancel()
