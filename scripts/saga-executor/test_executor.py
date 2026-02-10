from executor import SagaExecutor


# create executor
saga = SagaExecutor()

# dummy tasks
@saga.python_app()
def A(): return 1

@saga.python_app()
def B(): return 2

@saga.python_app()
def C(a, b): return a + b

# build graph
f0 = A()
f1 = B()
f2 = C(f0, f1)
f3 = C(f2, f1)
f4 = C(f3, f1)

# output graph
saga.print_graph(visualize=True)






# from saga.executor import PoolExecutor
# from saga.src.saga.schedulers.heft import HeftScheduler

# # Create a PoolExecutor (maybe print out task graph, make execute function)
# # Create Python App Decorator 


# executor = PoolExecutor(
#     scheduler=HeftScheduler()
# )

# @executor.python_app
# def task_light(task_id):
#     """Light task - 0.5 seconds"""
#     import time
#     import socket
#     start = time.time()
#     time.sleep(0.5)
#     elapsed = time.time() - start
#     hostname = socket.gethostname()
#     print(f"Task {task_id} (light, {elapsed:.2f}s) executed on {hostname}")
#     return (task_id, hostname, elapsed)

# @executor.python_app
# def task_heavy(task_id, prev_result):
#     """Heavy task - 2 seconds"""
#     import time
#     import socket
#     start = time.time()
#     time.sleep(2.0)
#     elapsed = time.time() - start
#     hostname = socket.gethostname()
#     print(f"Task {task_id} (heavy, {elapsed:.2f}s) executed on {hostname}, after {prev_result[0]}")
#     return (task_id, hostname, elapsed)

# @executor.python_app
# def task_medium(task_id, prev_result):
#     """Medium task - 1 second"""
#     import time
#     import socket
#     start = time.time()
#     time.sleep(1.0)
#     elapsed = time.time() - start
#     hostname = socket.gethostname()
#     print(f"Task {task_id} (medium, {elapsed:.2f}s) executed on {hostname}, after {prev_result[0]}")
#     return (task_id, hostname, elapsed)

# @executor.python_app
# def final_task(task_id, input1, input2):
#     """Final aggregation task"""
#     import time
#     import socket
#     start = time.time()
#     time.sleep(0.5)
#     elapsed = time.time() - start
#     hostname = socket.gethostname()
#     print(f"Task {task_id} (final, {elapsed:.2f}s) executed on {hostname}, aggregating inputs from {input1[0]} and {input2[0]}")
#     return (task_id, hostname, elapsed)


# def main():
#     executor.print_graph()

# if __name__ == "__main__":
#     main()