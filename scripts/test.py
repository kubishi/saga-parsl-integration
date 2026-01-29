import parsl
import os
from parsl.app.app import python_app
from parsl.config import Config
from parsl import dfk
from parsl.executors.threads import ThreadPoolExecutor
from parsl.providers import KubernetesProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_route
from parsl.usage_tracking.levels import LEVEL_1

from saga.schedulers.heft import HeftScheduler

# config = Config(
#     executors=[ThreadPoolExecutor()],
#     usage_tracking=LEVEL_1,
#     lazy_dfk=True,
#     saga_scheduler=HeftScheduler()
# )



provider = KubernetesProvider(
            namespace="default",
            image='nick23447/parsl-worker:latest',
            pod_name='parsl-worker',
            nodes_per_block=1,
            init_blocks=3,
            max_blocks=5,
            )


provider.get_nodes()

config = Config(
    executors=[HighThroughputExecutor(
        label='kube-htex',
        cores_per_worker=1,
        max_workers_per_node=1,
        worker_logdir_root='/tmp/parsl',
        address=address_by_route(),
        provider=KubernetesProvider(
            namespace="default",
            image='nick23447/parsl-worker:latest',
            pod_name='parsl-worker',
            nodes_per_block=1,
            init_blocks=3,
            max_blocks=5,
            ))],
    usage_tracking=LEVEL_1,
    lazy_dfk=True,
    saga_scheduler=HeftScheduler()
)



parsl.set_stream_logger() # <-- log everything to stdout

print(parsl.__version__)

parsl.load(config)

@python_app
def add(x, y):
    return x + y

@python_app
def multiply(x, y):
    return x * y

@python_app
def combine(a, b, c):
    return a + b + c

# Create a small workflow with dependencies
if __name__ == "__main__":
    print("Submitting tasks to demonstrate dependency graph...")

    a = add(1, 2)
    b = add(3, 4)
    
    c = multiply(a, b)

    print("\nFinal result:", c.result())
    
    # d = combine(a, b, c)

    for task in parsl.dfk().tasks.values():
        print("Task:", task)

        
    parsl.dfk().execute()

    print("\nFinal result:", c.result())
    parsl.dfk().cleanup()
    