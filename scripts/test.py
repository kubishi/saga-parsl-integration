"""
Kubernetes test for SAGA + Parsl integration.
Tests that SAGA scheduler generates task assignments and routes tasks to K8s worker pods.
"""
import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.providers import KubernetesProvider
from parsl.executors import HighThroughputExecutor
from parsl.addresses import address_by_route
from parsl.usage_tracking.levels import LEVEL_1
from saga.schedulers.heft import HeftScheduler

# Enable logging to see SAGA messages
parsl.set_stream_logger()

# K8s configuration with HighThroughputExecutor
config = Config(
    executors=[HighThroughputExecutor(
        label='kube-htex',
        cores_per_worker=1,
        max_workers_per_node=1,
        worker_logdir_root='/tmp/parsl',
        address=address_by_route(),
        encrypted=False,  # Disable encryption for local testing
        provider=KubernetesProvider(
            namespace="default",
            image='parsl-saga-worker:latest',
            image_pull_policy='Never',  # Use local image, don't pull from registry
            pod_name='parsl-worker',
            nodes_per_block=1,
            init_blocks=3,
            max_blocks=5,
        ))],
    usage_tracking=LEVEL_1,
    lazy_dfk=True,
    saga_scheduler=HeftScheduler()
)

print(f"Parsl version: {parsl.__version__}")

parsl.load(config)

@python_app
def add(x, y):
    import time
    import socket
    time.sleep(0.1)
    hostname = socket.gethostname()
    print(f"add({x}, {y}) = {x + y} executed on {hostname}")
    return x + y

@python_app
def multiply(x, y):
    import time
    import socket
    time.sleep(0.1)
    hostname = socket.gethostname()
    result = x * y
    print(f"multiply({x}, {y}) = {result} executed on {hostname}")
    return result

@python_app
def combine(a, b, c):
    import time
    import socket
    time.sleep(0.1)
    hostname = socket.gethostname()
    result = a + b + c
    print(f"combine({a}, {b}, {c}) = {result} executed on {hostname}")
    return result

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing SAGA + Parsl Integration (Kubernetes)")
    print("="*60 + "\n")

    print("Creating workflow with dependencies:")
    print("  a = add(1, 2)")
    print("  b = add(3, 4)")
    print("  c = multiply(a, b)")
    print("  d = combine(a, b, c)\n")

    # Create workflow with dependencies
    a = add(1, 2)
    b = add(3, 4)
    c = multiply(a, b)
    d = combine(a, b, c)

    print(f"Tasks created: {len(parsl.dfk().tasks)}")

    # Trigger SAGA scheduling by calling execute()
    print(f"\n{'='*60}")
    print("Invoking SAGA scheduler...")
    print(f"{'='*60}\n")
    parsl.dfk().execute()

    # Check if SAGA assignments were created
    if hasattr(parsl.dfk(), '_saga_task_assignments'):
        print(f"\n{'='*60}")
        print("SAGA Task Assignments:")
        print(f"{'='*60}")
        for task_id, node in parsl.dfk()._saga_task_assignments.items():
            print(f"  Task {task_id} -> Node {node}")
        print()
    else:
        print("\nWarning: No SAGA task assignments found!")
        print("This might indicate the scheduler wasn't invoked.\n")

    # Get results
    print(f"{'='*60}")
    print("Waiting for results...")
    print(f"{'='*60}\n")

    result_a = a.result()
    result_b = b.result()
    result_c = c.result()
    result_d = d.result()

    print(f"\nResults:")
    print(f"  a = add(1, 2) = {result_a}")
    print(f"  b = add(3, 4) = {result_b}")
    print(f"  c = multiply({result_a}, {result_b}) = {result_c}")
    print(f"  d = combine({result_a}, {result_b}, {result_c}) = {result_d}")

    expected = result_a + result_b + result_c
    print(f"\nExpected final result: {result_a} + {result_b} + {result_c} = {expected}")
    print(f"Actual final result: {result_d}")
    print(f"Test {'PASSED' if result_d == expected else 'FAILED'}!")

    print(f"\n{'='*60}")
    print("Test complete!")
    print(f"{'='*60}\n")

    parsl.dfk().cleanup()
    