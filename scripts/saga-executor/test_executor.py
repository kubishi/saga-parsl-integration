"""Test script for the SAGA multiprocessing executor."""

import logging
from executor import MultiprocessingExecutor
from saga.schedulers import HeftScheduler

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# -- Executor setup (module level for pickling) -----------------------------

executor = MultiprocessingExecutor(max_workers=4)


@executor.python_app(cost=0.5)
def constant(value: int) -> int:
    return value


@executor.python_app(cost=1.0)
def add(a: int, b: int) -> int:
    return a + b


@executor.python_app(cost=2.0)
def multiply(a: int, b: int) -> int:
    return a * b


# -- Build and run the graph inside __main__ for spawn safety --------------

if __name__ == "__main__":
    # Diamond-ish DAG:
    #   c1(3) ──┐
    #           add(c1,c2) ──┐
    #   c2(4) ──┘            │
    #                        add(sum, prod) -> final
    #   c3(5) ──┐            │
    #           mul(c3,c3) ──┘
    c1 = constant(3)
    c2 = constant(4)
    c3 = constant(5)

    sum_result = add(c1, c2)         # 3 + 4 = 7
    prod_result = multiply(c3, c3)   # 5 * 5 = 25
    final = add(sum_result, prod_result)  # 7 + 25 = 32

    executor.print_graph()
    executor.execute(HeftScheduler())

    print("\nResults:")
    print(f"  constant(3)    = {c1.result()}")
    print(f"  constant(4)    = {c2.result()}")
    print(f"  constant(5)    = {c3.result()}")
    print(f"  add(3, 4)      = {sum_result.result()}")
    print(f"  multiply(5, 5) = {prod_result.result()}")
    print(f"  add(7, 25)     = {final.result()}")
