"""
Local test for SAGA + Parsl integration without requiring Kubernetes.
Tests that SAGA scheduler generates task assignments.
"""
import parsl
from parsl.app.app import python_app
from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.usage_tracking.levels import LEVEL_1
from saga.schedulers.heft import HeftScheduler

# Enable logging to see SAGA messages
parsl.set_stream_logger()

# Simple config using ThreadPoolExecutor (no K8s required)
config = Config(
    executors=[ThreadPoolExecutor(max_threads=4, label='local')],
    usage_tracking=LEVEL_1,
    lazy_dfk=True,
    saga_scheduler=HeftScheduler()
)

print(f"Parsl version: {parsl.__version__}")

parsl.load(config)

@python_app
def add(x, y):
    import time
    time.sleep(0.1)  # Simulate some work
    return x + y

@python_app
def multiply(x, y):
    import time
    time.sleep(0.1)
    return x * y

@python_app
def combine(a, b, c):
    import time
    time.sleep(0.1)
    return a + b + c

if __name__ == "__main__":
    print("\n" + "="*60)
    print("Testing SAGA + Parsl Integration (Local Executor)")
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
