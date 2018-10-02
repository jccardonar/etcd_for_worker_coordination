import asyncio
import threading
import time
from asyncio import InvalidStateError
from collections import namedtuple

import etcd3
from common_libraries import (CustomAdapter, calculate_job_distribution,
                              get_etcd_prefix)

# global_task_prefix = queue_prefix

CoordinatorConfiguration = namedtuple("CoordinatorConfiguration", [
    "uuid", "group_prefix", "workers_subprefix", "global_task_prefix",
    "coordinator_subprefix", "worker_queue_subprefix",
    "coordinator_time_to_renew"
])


class Coordinator(object):
    '''
    Coordinator's task:
    The coordinator keeps on sync the general task queue with the queues of the workers.
    Workers can appear and dissapear. Same with tasks.
    Coordinator Election:
    There can only be one coordinator. All candidates try to lock a key on etcd.
    Whoever has it becomes the coordinator.
    See the run function for more detail.
    '''

    def __init__(self, coordinator_configuration, etcd_configuration, loop,
                 logger):

        self.uuid = coordinator_configuration.uuid
        self.loop = loop
        self.etcd = etcd3.client(
            host=etcd_configuration.etcd_address,
            port=etcd_configuration.etcd_port)

        self.logger = CustomAdapter(
            logger,
            {'module_id': self.__class__.__name__ + "." + str(self.uuid)})

        self.coordinator_configuration = coordinator_configuration

        # Variables used for coordinator election
        self.coordinator_lock = None
        self.refresh_coordinator_state = True

        # task and worker state and events
        self.workers = None
        self.task_list = None
        self.tasks_per_worker = None

        self.lock_processing = threading.Lock()

        self.stop = False

    # Worker list management functions
    # ====
    def keep_workers_state(self, event):
        '''
        Maintains the list of available workers by watching a etcd prefix.
        '''
        prefix = etcd3.utils.to_bytes(
            self.coordinator_configuration.group_prefix + "." +
            self.coordinator_configuration.workers_subprefix + ".")
        worker_list_event_loop, self.worker_list_cancel = self.etcd.watch_prefix(
            prefix)
        workers = {}
        try:
            workers = set([x for x in get_etcd_prefix(self.etcd, prefix)])
        except:
            raise Exception("Failed etcd connection when fetching workers")
        with self.lock_processing:
            self.workers = workers
        self.logger.debug("Found the next workers: %s", self.workers)
        for etcd_event in worker_list_event_loop:
            self.logger.debug("Receiving worker list event %s", etcd_event)
            key = etcd_event.key.decode()
            # usually you would recheck that the key starts with the prefix
            workerid = key.replace(prefix.decode(), "")
            if isinstance(etcd_event, etcd3.events.PutEvent):
                self.logger.debug("New worker put event is %s", etcd_event)
                with self.lock_processing:
                    self.workers.add(workerid)
                continue
            # print(etcd_event)
            elif isinstance(etcd_event, etcd3.events.DeleteEvent):
                self.logger.debug("New worker del event is %s", etcd_event)
                with self.lock_processing:
                    self.workers.discard(workerid)
                continue

        self.logger.debug("Leaving workers list state cycle")

    # Global tasks list management functions
    # =====
    def keep_tasks_list_state(self, event):
        '''
        Maintains the list of all required tasks by watching the required etcd prefix.
        '''
        prefix = etcd3.utils.to_bytes(
            self.coordinator_configuration.global_task_prefix + ".")
        tasks_list_event_loop, self.tasks_list_cancel = self.etcd.watch_prefix(
            prefix)
        tasks = {}
        try:
            tasks = set([x for x in get_etcd_prefix(self.etcd, prefix)])
        except:
            raise Exception("Failed etcd connection when fetching tasks")
        with self.lock_processing:
            self.task_list = tasks
        self.logger.debug("Found the next tasks: %s", self.task_list)
        for etcd_event in tasks_list_event_loop:
            self.logger.debug("Receiving tasks list event %s", etcd_event)
            key = etcd_event.key.decode()
            # usually you would recheck that the key starts with the prefix
            taskid = key.replace(prefix.decode(), "")
            if isinstance(etcd_event, etcd3.events.PutEvent):
                self.logger.debug("New task put event is %s", etcd_event)
                with self.lock_processing:
                    self.task_list.add(taskid)
                continue
            # print(etcd_event)
            elif isinstance(etcd_event, etcd3.events.DeleteEvent):
                self.logger.debug("New task del event is %s", etcd_event)
                with self.lock_processing:
                    self.task_list.discard(taskid)
                continue

        self.logger.debug("Leaving tasks list state cycle")

    # Workers queues management function
    # ======
    def keep_workers_task_state(self, event):
        prefix = etcd3.utils.to_bytes(
            self.coordinator_configuration.group_prefix + "." +
            self.coordinator_configuration.worker_queue_subprefix + ".")
        worker_task_list_event_loop, self.worker_task_cancel = self.etcd.watch_prefix(
            prefix)
        with self.lock_processing:
            self.tasks_per_worker = {}
        try:
            tasks_per_worker = get_etcd_prefix(self.etcd, prefix)
        except:
            raise Exception("Failed etcd connection when fetching workers")
        for task_per_worker in tasks_per_worker:
            divided = task_per_worker.decode().split(".")
            if len(divided) != 2:
                self.logger.warning(
                    "key in worker queue %s has not the right format",
                    task_per_worker)
                continue
            worker = divided[0]
            task = divided[1]
            with self.lock_processing:
                self.tasks_per_worker.setdefault(worker, set()).add(task)
        self.logger.debug("Found next worker tasks: %s", self.tasks_per_worker)

        # now keep track of any changes.
        for etcd_event in worker_task_list_event_loop:
            self.logger.debug("Receiving task worker list event %s",
                              etcd_event)
            key = etcd_event.key.decode()
            work_task_id = key.replace(prefix.decode(), '')
            divided = work_task_id.split(".")
            if len(divided) != 2:
                self.logger.warning(
                    "key in worker queue %s has not the right format", key)
                continue
            worker = divided[0]
            task = divided[1]
            if isinstance(etcd_event, etcd3.events.PutEvent):
                self.logger.debug("New worker put event is %s", etcd_event)
                with self.lock_processing:
                    self.tasks_per_worker.setdefault(worker, set()).add(task)
                continue
            # print(etcd_event)
            elif isinstance(etcd_event, etcd3.events.DeleteEvent):
                self.logger.debug("New worker del event is %s", etcd_event)
                with self.lock_processing:
                    self.tasks_per_worker.setdefault(worker,
                                                     set()).discard(task)
                continue

        self.logger.debug("Leaving workers task list state cycle")

    # Workers queue calculation functions
    # =====
    async def keep_workers_queues(self):
        while True:
            await asyncio.sleep(5)
            if not self.am_I_coordinator():
                continue
            self.logger.debug("Trying to get lock to process queues")
            with self.lock_processing:
                if self.tasks_per_worker is None or self.task_list is None or self.workers is None:
                    self.logger.error(
                        "We are coordinators, but one of self.tasks_per_worker, self.task_list, self.workers is None. Values %s, %s, %s",
                        self.tasks_per_worker, self.task_list, self.workers)
                    continue

                # the calculate_job_distribution is the core of the scheduler function.
                # I added it to the common library function, since it can be used for other purposes.
                try:
                    n_state, state_additions, state_deletes, state_switches = calculate_job_distribution(
                        self.tasks_per_worker,
                        self.task_list,
                        self.workers,
                        rebalance=True)
                except Exception as e:
                    self.logger.error(
                        "Failed at distributing tasks with error %s", e)
                    raise
                else:
                    self.logger.debug(
                        "Changes for state: state_additions: %s, state_deletes: %s, state_switches: %s",
                        state_additions, state_deletes, state_switches)

            # now let us make the changes, only if we are coordinator
            if not self.am_I_coordinator():
                continue
            await self.update_etcd(state_additions, state_deletes,
                                   state_switches)

    async def update_etcd(self, state_additions, state_deletes,
                          state_switches):
        # Performs the changes in etcd in a single transaction.
        # get a list of puts
        puts = []
        workers_queue_prefix = self.coordinator_configuration.group_prefix + \
            "." + self.coordinator_configuration.worker_queue_subprefix
        for worker in state_additions:
            for task in state_additions[worker]:
                prefix = workers_queue_prefix + "." + worker + "." + task
                transaction = self.etcd.transactions.put(prefix, task)
                puts.append(transaction)
        deletes = []
        for worker in state_deletes:
            for task in state_deletes[worker]:
                prefix = workers_queue_prefix + "." + worker + "." + task
                transaction = self.etcd.transactions.delete(prefix)
                deletes.append(transaction)

        for switches in state_switches:
            for task in state_switches[switches]:
                worker_remove = switches[0]
                worker_add = switches[1]
                prefix_put = workers_queue_prefix + "." + worker_add + "." + task
                # Create the put transaction (adding to the new worker)
                transaction = self.etcd.transactions.put(prefix_put, task)
                puts.append(transaction)
                # Create the delete transaction (deleting from the old  worker)
                prefix_delete = workers_queue_prefix + "." + worker_remove + "." + task
                transaction = self.etcd.transactions.delete(prefix_delete)
                deletes.append(transaction)

        self.logger.debug(
            "Attempting to do transaction with puts {} and deletes {}".format(
                puts, deletes))
        tranactions = puts + deletes
        if not tranactions:
            self.logger.debug("No transaction needed")
            return

        # this is a transaction, so, although it blocks, it should not take too long for this prototype.
        try:
            result = self.etcd.transaction(
                compare=[], success=tranactions, failure=[])
        except Exception as e:
            self.logger.error("Error on transaction: %s", e)
        else:
            self.logger.debug("Return of transaction is %s", result)

    # Coordinator election functions
    # =============
    def am_I_coordinator(self):
        if self.coordinator_lock is not None:
            if self.coordinator_lock.is_acquired():
                return True
        return False

    def is_there_coordinator(self):
        '''
        Checks if there is a coordinator defined. Uses the prefix for the lock.
        After it has checked. It should watch the prefix for any issue.
        '''
        coordinator_content = self.etcd.get(
            self.coordinator_configuration.coordinator_subprefix)
        if coordinator_content[0] is None and coordinator_content[1] is None:
            return False
        return True

    def coordinator_keeper_cycle(self, event):
        while True:
            if event.is_set():
                self.logger.debug("Exiting coordinator periodic check process")
                return
            self.register_refresh_for_coordinator()
            time.sleep(self.coordinator_configuration.coordinator_time_to_renew
                       * 2 / 3)
        return

    def coordinator_loop(self, event):
        self.logger.info("Starting coordination election process")
        # we subscribe to the coordinator prefix
        coordinator_event_loop, self.coordinator_watch_cancel = self.etcd.watch_prefix(
            self.coordinator_configuration.coordinator_subprefix)

        # we now luch the coordinator keepers cycle
        coordinator_future = self.loop.run_in_executor(
            None, self.coordinator_keeper_cycle, self.event_cancel_coordinator)

        # we listen for events, in order to react quickly if the coordinator leaves.
        for etcd_event in coordinator_event_loop:
            self.register_refresh_for_coordinator()

        self.logger.debug("Exiting coordinator loop")
        return

    def register_refresh_for_coordinator(self):
        '''
        This function is used to get (or refresh) the coordinator token.
        Returns True if the token is obtained, false if not
        '''
        # A lock is needed for this. The code in the library is not great, and
        # there is not callback for when the lock is released. etcd3 already has a
        # api for this, but it is not used in the library.
        if self.coordinator_lock is not None:
            # we used to have the lock, let us try to refresh it.
            if self.coordinator_lock.is_acquired():
                try:
                    self.coordinator_lock.refresh()
                    self.logger.debug("Master locked refreshed.")
                    return True
                except:
                    self.logger.warning(
                        "We could not refresh coordinator lock. We lost it")
                    self.coordinator_lock = None
            else:
                self.logger.debug("We lost the coordinator lock.")
                self.coordinator_lock = None

        # we do not have the lock, try to get it
        if self.coordinator_lock is None:
            self.logger.debug("Attempting to acquire coordinator lock")
            lock = self.etcd.lock(
                self.coordinator_configuration.coordinator_subprefix,
                ttl=self.coordinator_configuration.coordinator_time_to_renew)
            # try to acquire it
            succeed = lock.acquire(1)
            if succeed:
                self.logger.debug("Master lock acquired")
                self.coordinator_lock = lock
                return True
            else:
                return False

    # Coordinator control functions
    # ====
    async def run(self):
        '''
        Starts the coordinator.
        The coordinator various threads:
        -coordinator election proccess (blocks)
        -keeping track of the workers (blocks)
        -keeping track of the tasks (blocks)
        -keeping track of the tasks per worker (useful for when the agent is not the coordinator) (blocks)
        -Syncing the desired load per worker with the actual tasks per worker, by calculating the "right" load.

        The four later are only performed if the agent is the coordinator.

        The coordinator maintains internal data structures with the info of:
        -global tasks
        -workers
        -task per worker

        The coordinator acts as an scheduler, getting the overall task and workers
        and scheduling which tasks each worker should handle.

        After a short period, the coordinator (if elected) syncs the three.
        This includes making sure all tasks are handle, and that the "balance" of task is "effitient".
        TODO: We can also perform the sync after a change by implementing a signal.

        For this prototype, the balancing is just based on number of tasks.
        '''
        self.logger.info("Starting to run coordination")

        # single event to signal cancelation.
        self.event_cancel_coordinator = threading.Event()

        # coordination election management thread
        coordinator_future = self.loop.run_in_executor(
            None, self.coordinator_loop, self.event_cancel_coordinator)

        # we run the processes keeping the queues for workers and tasks
        # global task management thread
        task_list_future = self.loop.run_in_executor(
            None, self.keep_workers_state, self.event_cancel_coordinator)
        # workers management thread
        workers_list_future = self.loop.run_in_executor(
            None, self.keep_tasks_list_state, self.event_cancel_coordinator)
        # tasks per worker management thread
        worker_task_list_future = self.loop.run_in_executor(
            None, self.keep_workers_task_state, self.event_cancel_coordinator)

        # task per worker calculation (this one does not block, it is a typical coroutine)
        keep_workers_queue = self.keep_workers_queues_task = self.loop.create_task(
            self.keep_workers_queues())

        self.stop = False
        # just keep the process alive until it is cancelled.
        self.logger.info("Entering control loop")
        while not self.stop:
            for task in [
                    coordinator_future, task_list_future, workers_list_future,
                    worker_task_list_future, keep_workers_queue
            ]:
                try:
                    if task.exception() is not None:
                        raise task.exception()
                except InvalidStateError:
                    pass
                except:
                    raise

            await asyncio.sleep(10)

    def cancel(self):
        try:
            self.keep_workers_queues_task.cancel()
        except:
            pass

        try:
            self.coordinator_watch_cancel()
        except:
            pass
        try:
            self.event_cancel_coordinator.set()
        except:
            pass
        try:
            self.worker_list_cancel()
        except:
            pass
        try:
            self.tasks_list_cancel()
        except:
            pass

        try:
            self.worker_task_cancel()
        except:
            pass

        self.stop = True
