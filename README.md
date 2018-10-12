# Brief description
This repo contains an example of using Etcd3 and the python asyncio library for worker coordination.

# Longer description
Orchestration systems (such as Kubernetes) might support auto-scaling features. These features allows the orchestrator to scale down or up the number of containers that form the app to handle different amounts of load.

After the orchestrator scales the number of containers up or down, it is a responsibility of the app to redistribute load across them.

Some apps can use a load balancer or message broker (i.e. Kafka) to distribute load automatically across the application containers.
Other type of applications require workers to perform long lasting tasks that cannot be so easily distributed.

The code on this repo is a prototype of functions for the agents forming app of the latter kind to sync up how they should split the tasks.

The functionality included in the code is:
- Worker discovery
- Coordinator selection
- Task load balancing and assignment

## How it works

1. Another system uses etcd keys and values to define the task that the workers must do.

2. Workers start and register themselves in a etcd prefix with a lease. They MUST maintain the lease to show that they are alive. They each watch an etcd prefix in which their tasks are placed (by the coordinator, see next).

3. A coordinator is elected by registering itself in a well known key with a lock. The library does not use the lock feature from etcd3 yet.

4. The coordinator reads the tasks for the cluster, and reads the tasks assigned to each worker (workers are the ones that registered themselves in point 1.). It recalculates the ideal distribution of tasks per worker at each cycle. An single etcd transaction is used modifying task assignment (switching, creating, deleting).


## Code description
The code includes the independent objects for the workers and the coordinator agents. Normally, each element of the cluster should play both roles.

# Requirements
pip install -r requirements

