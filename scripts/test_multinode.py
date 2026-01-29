"""
Multi-node Kubernetes test for SAGA + Parsl integration.
Uses a diamond-shaped DAG with varying task costs to encourage distribution.
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
            init_blocks=4,  # Start with 4 blocks for 4 nodes
            max_blocks=6,
        ))],
    usage_tracking=LEVEL_1,
    lazy_dfk=True,
    saga_scheduler=HeftScheduler()
)

print(f"Parsl version: {parsl.__version__}")

parsl.load(config)

@python_app
def task_light(task_id):
    """Light task - 0.5 seconds"""
    import time
    import socket
    start = time.time()
    time.sleep(0.5)
    elapsed = time.time() - start
    hostname = socket.gethostname()
    print(f"Task {task_id} (light, {elapsed:.2f}s) executed on {hostname}")
    return (task_id, hostname, elapsed)

@python_app
def task_heavy(task_id, prev_result):
    """Heavy task - 2 seconds"""
    import time
    import socket
    start = time.time()
    time.sleep(2.0)
    elapsed = time.time() - start
    hostname = socket.gethostname()
    print(f"Task {task_id} (heavy, {elapsed:.2f}s) executed on {hostname}, after {prev_result[0]}")
    return (task_id, hostname, elapsed)

@python_app
def task_medium(task_id, prev_result):
    """Medium task - 1 second"""
    import time
    import socket
    start = time.time()
    time.sleep(1.0)
    elapsed = time.time() - start
    hostname = socket.gethostname()
    print(f"Task {task_id} (medium, {elapsed:.2f}s) executed on {hostname}, after {prev_result[0]}")
    return (task_id, hostname, elapsed)

@python_app
def final_task(task_id, input1, input2):
    """Final aggregation task"""
    import time
    import socket
    start = time.time()
    time.sleep(0.5)
    elapsed = time.time() - start
    hostname = socket.gethostname()
    print(f"Task {task_id} (final, {elapsed:.2f}s) executed on {hostname}, aggregating inputs from {input1[0]} and {input2[0]}")
    return (task_id, hostname, elapsed)

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing SAGA + Parsl Multi-Node Distribution")
    print("="*60 + "\n")

    print("Creating diamond-shaped DAG with varying task costs:")
    print("  t1 (light) - entry point")
    print("  t2 (heavy, depends on t1) - long computation")
    print("  t3 (medium, depends on t1) - medium computation")
    print("  t4 (light, depends on t2, t3) - aggregation\n")

    # Diamond-shaped DAG:
    #     t1 (light)
    #    /  \
    #   t2   t3
    # (heavy)(medium)
    #    \  /
    #     t4 (light)

    t1 = task_light("t1")
    t2 = task_heavy("t2", t1)  # Depends on t1
    t3 = task_medium("t3", t1) # Depends on t1
    t4 = final_task("t4", t2, t3)  # Depends on t2 and t3

    print(f"Tasks created: {len(parsl.dfk().tasks)}")

    # Trigger SAGA scheduling
    print(f"\n{'='*60}")
    print("Invoking SAGA scheduler...")
    print(f"{'='*60}\n")
    parsl.dfk().execute()

    # Check SAGA assignments
    if hasattr(parsl.dfk(), '_saga_task_assignments'):
        print(f"\n{'='*60}")
        print("SAGA Task Assignments:")
        print(f"{'='*60}")
        for task_id, node in sorted(parsl.dfk()._saga_task_assignments.items()):
            print(f"  Task {task_id} -> Node {node}")
        print()
    else:
        print("\nWarning: No SAGA task assignments found!\n")

    # Get results
    print(f"{'='*60}")
    print("Waiting for results...")
    print(f"{'='*60}\n")

    import time as timer
    start_time = timer.time()
    r1 = t1.result()
    r2 = t2.result()
    r3 = t3.result()
    r4 = t4.result()
    total_time = timer.time() - start_time

    print(f"\n{'='*60}")
    print("Execution Summary:")
    print(f"{'='*60}")
    print(f"Task t1 (light):  {r1[1]} - {r1[2]:.2f}s")
    print(f"Task t2 (heavy):  {r2[1]} - {r2[2]:.2f}s")
    print(f"Task t3 (medium): {r3[1]} - {r3[2]:.2f}s")
    print(f"Task t4 (final):  {r4[1]} - {r4[2]:.2f}s")
    print(f"\nTotal execution time: {total_time:.2f}s")

    # Check if tasks were distributed
    nodes_used = set([r1[1], r2[1], r3[1], r4[1]])
    print(f"Nodes used: {len(nodes_used)} ({', '.join(sorted(nodes_used))})")

    if len(nodes_used) > 1:
        print("✅ Tasks were distributed across multiple nodes!")
    else:
        print("⚠️  All tasks ran on a single node")

    print(f"\n{'='*60}")
    print("Test complete!")
    print(f"{'='*60}\n")

    parsl.dfk().cleanup()
