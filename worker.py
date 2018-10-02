import asyncio
import itertools
import threading
import time
from asyncio import InvalidStateError
from collections import namedtuple

import aiofiles

import etcd3
from common_libraries import CustomAdapter, get_etcd_prefix

WorkerConfiguration = namedtuple("WorkerConfiguration", [
    "uuid",
    "group_prefix",
    "workers_subprefix",
    "worker_queue_subprefix",
    "worker_time_to_renew",
    "result_file",
])


class Worker():
    '''
    The worker has three main objectives.
    - Register itself and keep the registry alive.
    - Keep track of the tasks it should perform over etcd.
    - Perform the tasks
    '''

    def __init__(self, worker_configuration, etcd_configuration, loop, logger):
        self.uuid = worker_configuration.uuid

        self.loop = loop
        self.logger = CustomAdapter(
            logger,
            {'module_id': self.__class__.__name__ + "." + str(self.uuid)})

        self.etcd = etcd3.client(
            host=etcd_configuration.etcd_address,
            port=etcd_configuration.etcd_port)

        self.worker_configuration = worker_configuration

        self.worker_lease = None
        workers_prefix = worker_configuration.group_prefix + \
            "." + worker_configuration.workers_subprefix
        self.worker_prefix = workers_prefix + "." + self.uuid
        self.tasks_prefix = worker_configuration.group_prefix + "." + \
            worker_configuration.worker_queue_subprefix + "." + self.uuid

        # task and worker state and events
        self.tasks = {}

        # this lock helps coordinate threads when changing non-thread safe data structures.
        self.lock_processing = threading.Lock()

        self.event_cancel_worker = threading.Event()

        self.logger.info("Creating worker %s", self.uuid)
        self.stop = True

    # Refresh keepalive functions
    # ======
    def register_refresh_worker(self):
        '''
        Registers or refreshes the keepalive registry of the worker in etcd.
        '''
        # If worker lease variable is set, then refresh it.
        if self.worker_lease is not None:
            self.logger.debug("Refreshing worker prefix")
            return_refresh = self.worker_lease.refresh()
            if return_refresh[
                    0].TTL != self.worker_configuration.worker_time_to_renew:
                self.logger.warning("We could not refresh worker prefix")
                # this did not work, let us renew this
                self.worker_lease = None

        # If lease not yet established (or if it failed), then start the key lease.
        if self.worker_lease is None:
            self.logger.debug("Acquiring worker prefix")
            self.worker_lease = self.etcd.lease(
                self.worker_configuration.worker_time_to_renew)
            self.etcd.put(
                self.worker_prefix, self.uuid, lease=self.worker_lease)

    def mantain_worker_prefix(self, cancel_event):
        '''
        Keeps up the worker process, and stops it if necessary.
        '''
        while True:
            # if cancel event is set, then stop worker process.
            if cancel_event.is_set():
                self.logger.debug("Exiting worker keepalive process")
                return
            self.register_refresh_worker()

            # Sleep some time, but not too much.
            time.sleep(self.worker_configuration.worker_time_to_renew * 2 / 3)
        return

    # Tasks handleling functions
    # ==============
    def handle_tasks_queue(self, event):
        '''
        Handles the tasks queue that the worker must perform.
        Tasks are defined by self.process_task an a task id.
        '''
        # Watch the task prefix.
        task_event_loop, self.handle_taskscancel = self.etcd.watch_prefix(
            self.tasks_prefix + ".")

        # Get current set of tasks, initialize all of them
        tasks = get_etcd_prefix(self.etcd, self.tasks_prefix + ".")
        for task in tasks:
            self.logger.debug("Creating task %s", task)
            task_future = self.loop.create_task(self.process_task(task))
            # do not mess with the self.tasks without the lock
            with self.lock_processing:
                if task in self.tasks:
                    self.tasks[task].cancel()
                self.tasks[task] = task_future

        # for every event, check what we need to do (remove or add)
        for etcd_event in task_event_loop:
            self.logger.debug("Receiving worker task event %s", etcd_event)
            if isinstance(etcd_event, etcd3.events.PutEvent):
                self.logger.debug("New worker task put event is %s",
                                  etcd_event)
                # add a task
                key = etcd_event.key.decode()
                # usually you would recheck that the key starts with the prefix
                task_id = key.replace(self.tasks_prefix + ".", "")
                if task_id in self.tasks:
                    self.logger.debug("task id %s already exists, ignoring",
                                      etcd_event)
                    continue
                # if not, create it
                self.logger.debug("Creating task %s", etcd_event)
                task_future = self.loop.create_task(self.process_task(task_id))
                # do not mess with the self.tasks without the lock
                with self.lock_processing:
                    self.tasks[task_id] = task_future
                continue
            elif isinstance(etcd_event, etcd3.events.DeleteEvent):
                self.logger.debug("New worker task delete event is %s",
                                  etcd_event)
                # add a task
                key = etcd_event.key.decode()
                # usually you would recheck that the key starts with the prefix
                task_id = key.replace(self.tasks_prefix + ".", "")
                if task_id not in self.tasks:
                    self.logger.error("task id %s does not exist in set.",
                                      etcd_event)
                    continue
                self.logger.debug("Canceling task %s", etcd_event)
                with self.lock_processing:
                    self.tasks[task_id].cancel()
                    del self.tasks[task_id]
                continue
            else:
                self.logger.warning("Unrecognized worker event event %s",
                                    etcd_event)

        self.logger.debug("Exiting handle tasks")
        return

    # Currenlty, tasks are just sleeps. But they could be something more complex
    # as long as they are
    async def process_task(self, task_id):
        '''
        Processes a task with a specific task_id.
        In a more complex case, the task id would normally point to something else
        with a complex description (such as another etcd prefix).
        '''
        while True:
            self.logger.info("Collecting {} using Worker {}".format(
                task_id, self.uuid))
            data = ','.join([str(time.time()), task_id, self.uuid]) + "\n"
            async with aiofiles.open(self.worker_configuration.result_file,
                                     'a') as f:
                await f.write(data)

            # Let us wait sometime.
            await asyncio.sleep(10)

    # Worker control functions
    # ===========

    def cancel(self):
        '''
        Cancels worker functions
        '''
        self.logger.info("Cancelling functions")
        # cancel worker functions
        try:
            self.event_cancel_worker.set()
        except:
            pass

        # cancel any etcd watching functions
        try:
            self.handle_taskscancel()
        except:
            pass

        with self.lock_processing:
            for task in self.tasks:
                self.tasks[task].cancel()
        self.stop = True

    async def run(self):
        '''
        Runs worker functions.
        '''
        self.stop = False
        # etcd blocks, so we need a thread
        # both these threads can be cancel with a single event
        self.worker_future = self.loop.run_in_executor(
            None, self.mantain_worker_prefix, self.event_cancel_worker)
        self.task_future = self.loop.run_in_executor(
            None, self.handle_tasks_queue, self.event_cancel_worker)

        # Loop checking for issues
        while not self.stop:

            # Go over all task functions, checking if there is any exception.
            # TODO: self.lock_processing blocks. This is not good here, since it also blocks
            # the tasks which are coroutines. We rely on the other functions to be on threads.
            with self.lock_processing:
                for task in itertools.chain(self.tasks.values(), [self.worker_future, self.task_future]):
                    try:
                        if task.exception() is not None:
                            raise task.exception()
                    except InvalidStateError:
                        pass
                    except:
                        raise
            await asyncio.sleep(5)
