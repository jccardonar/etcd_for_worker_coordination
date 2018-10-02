import copy
import logging
from collections import namedtuple

import etcd3

EtcdConfig = namedtuple("EtcdConfig", ["etcd_address", "etcd_port"])


class CustomAdapter(logging.LoggerAdapter):
    """
    This example adapter expects the passed in dict-like object to have a
    'module_id' key, whose value in brackets is prepended to the log message.
    """

    def process(self, msg, kwargs):
        return '[%s] %s' % (self.extra['module_id'], msg), kwargs


def get_etcd_prefix(etcd_h, key):
    # returns the subprefixes (without the dot)
    # If needed, add pagination here.
    key = etcd3.utils.to_bytes(key)

    try:
        etcd_items = etcd_h.get_prefix(key)
    except:
        raise Exception("Failed etcd connection when fetching prefix")

    items = set()
    for etcd_item in etcd_items:
        item_prefix = etcd_item[1].key
        itemid = (item_prefix.replace(key, b"")).decode('utf-8')
        items.add(itemid)
    return items


def calculate_job_distribution(c_state, c_tasks, c_workers, rebalance=False):
    '''
    Calculates a new task distribution based on the current state, and the current tasks.
    c_state is a dict[worker]->set(tasks). Worker and tasks are strings or ints.
    c_tasks is a set of tasks.
    If asked, it will try to rebalance the tasks. The balance mechanims is a simple heuristic.
    '''

    # calculate remove and added workers
    all_workers = set(c_state)
    new_workers = set(c_workers) - all_workers
    deleted_workers = all_workers - set(c_workers)

    # get unassigned and deleted tasks.
    all_assigned_tasks = set(task for worker, tasks in c_state.items() for task in tasks)
    unassigned_tasks = set(c_tasks) - all_assigned_tasks
    deleted_tasks = all_assigned_tasks - set(c_tasks)

    # Here we do the tasks distribution. If needed, the function can include all the sophisticated features such as:
    # - Feature time vector per worker (all attributes useful to calcualte load in time)
    # - task load predictor (something to predict load of each task)
    # meanwhile, we'll use something simple
    return distribute_load(c_state, unassigned_tasks, deleted_tasks,
                           new_workers, deleted_workers, rebalance)


def distribute_load(c_state,
                    unassigned_tasks,
                    deleted_tasks,
                    new_workers,
                    deleted_workers,
                    rebalance=False):
    n_state = copy.deepcopy(c_state)

    state_deletes = {}
    state_additions = {}
    state_switches = {}

    # delete worker
    for worker in deleted_workers:
        state_deletes[worker] = n_state[worker]
        try:
            del n_state[worker]
        except:
            pass

    # add new workers
    for worker in new_workers:
        n_state[worker] = set()

    # delete tasks first
    for worker in n_state:
        state_deletes[worker] = n_state[worker] & deleted_tasks
        n_state[worker] = n_state[worker] - deleted_tasks

    # The next loop is ugly in performance, but easy to read. We can definitly improve  it, but this will work for now.
    for task in unassigned_tasks:
        tasks_per_worker = {}
        for worker in n_state:
            tasks_per_worker.setdefault(len(n_state[worker]),
                                        set()).add(worker)
        sorted_tasks = sorted(tasks_per_worker)
        worker = next(iter(tasks_per_worker[sorted_tasks[0]]))
        n_state[worker].add(task)
        state_additions.setdefault(worker, set()).add(task)

    if rebalance:
        # Now, if needed, distribute, this will reduce variane, one by one, with a limit of limit_loops as safeguard.
        limit_loops = 100
        counter = 0
        while counter < limit_loops:
            tasks_per_worker = {}
            for worker in n_state:
                tasks_per_worker.setdefault(len(n_state[worker]),
                                            set()).add(worker)
            sorted_tasks = sorted(tasks_per_worker)
            min_value = sorted_tasks[0]
            max_value = sorted_tasks[-1]
            if max_value - min_value < 2:
                # we cannot do any better
                break
            # take a task from a task in the maximum and give it to the minimum.
            worker_max = next(iter(tasks_per_worker[max_value]))
            worker_min = next(iter(tasks_per_worker[min_value]))
            task_to_switch = next(iter(n_state[worker_max]))
            n_state[worker_max].remove(task_to_switch)
            n_state[worker_min].add(task_to_switch)
            state_switches.setdefault((worker_max, worker_min),
                                      set()).add(task_to_switch)
            counter = counter + 1

    return n_state, state_additions, state_deletes, state_switches
