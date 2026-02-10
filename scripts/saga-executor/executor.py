from typing import Optional, Callable, Any, Dict, List
from functools import wraps
import concurrent.futures
import networkx as nx
#from saga.src.saga import Scheduler
import matplotlib.pyplot as plt


"""
Notes:
- if user adds arguements in function call but not when defining the function, 
we can just ignore those extra arguments for the graph construction (they won't be used by the scheduler)

"""

class AppFuture:
    """
    A placeholder for a result that hasn't been computed yet.
    It links a variable in the user's script to a node in the Task Graph.
    """
    def __init__(self, task_id: str, executor: 'SagaExecutor'):
        self.task_id = task_id
        self._executor = executor
        self._result = None
        self._computed = False

    def result(self):
        """
        If the graph has already run, return the result.
        If not, this blocks or raises an error depending on your preference.
        """
        if not self._computed:
            # OPTION A: Auto-trigger execution (complex, but user-friendly)
            # self._executor.execute() 
            
            # OPTION B: Strict Offline (safer for static scheduling)
            raise RuntimeError("Task not computed yet. Call executor.execute() first.")
        return self._result
        
    def _set_result(self, value):
        self._result = value
        self._computed = True


class SagaExecutor:
    def __init__(self):
        # The Graph
        self.task_graph = nx.DiGraph()
        
        # The Registry: Stores the actual function code and raw arguments for later
        self._task_registry = {} 
        self._task_counter = 0
        
        # Map task_ids to their AppFuture instances so we can update them later
        self._future_registry = {}

    def python_app(self, cost: Optional[float] = None):
        """
        Decorator that builds the graph node and returns a Symbolic Future.
        """
        def decorator(func: Callable):
            @wraps(func)
            def wrapper(*args, **kwargs) -> AppFuture:
                # 1. Generate unique ID
                task_id = f"task_{self._task_counter}"
                self._task_counter += 1
                
                # 2. Extract Dependencies
                deps = self._extract_dependencies(args, kwargs)
                
                # 3. Build Graph Structure
                self.task_graph.add_node(task_id, name=func.__name__, cost=cost)
                for dep_future in deps:
                    self.task_graph.add_edge(dep_future.task_id, task_id)
                
                # 4. Register the Task 
                self._task_registry[task_id] = {
                    'func': func,
                    'args': args,
                    'kwargs': kwargs
                }
                
                # 5. Create and return a Symbolic Future
                future = AppFuture(task_id, self)
                self._future_registry[task_id] = future
                return future
                
            return wrapper
        return decorator

    def _extract_dependencies(self, args, kwargs) -> List[AppFuture]:
        """Finds all AppFuture objects in the arguments to identify parents."""
        deps = []
        for arg in args:
            if isinstance(arg, AppFuture):
                deps.append(arg)
        for val in kwargs.values():
            if isinstance(val, AppFuture):
                deps.append(val)
        return deps

    # --- 3. The Execution Phase ---
    def execute(self):
        """
        1. Run Offline Scheduling Algo (HEFT)
        2. Execute tasks in order
        3. Resolve arguments (Futures -> Real Values)
        """
        if not self.task_graph.nodes:
            print("No tasks to run.")
            return

        # A. SCHEDULING STEP
        # (Placeholder for your SAGA/HEFT logic)
        # For now, we just get a valid topological order to ensure parents run before children
        execution_order = list(nx.topological_sort(self.task_graph))
        print(f"Calculated Schedule: {execution_order}")

        # B. EXECUTION LOOP
        results_cache = {} # Local storage for results to pass to children

        for task_id in execution_order:
            task_info = self._task_registry[task_id]
            func = task_info['func']
            
            # C. ARGUMENT RESOLUTION
            # We must swap AppFutures for their actual computed results
            final_args = [
                arg.result() if isinstance(arg, AppFuture) else arg 
                for arg in task_info['args']
            ]
            final_kwargs = {
                k: (v.result() if isinstance(v, AppFuture) else v)
                for k, v in task_info['kwargs'].items()
            }
            
            # D. RUN ACTUAL TASK (This is where you'd send to K8s/Worker)
            print(f"Running {task_id}...")
            # actual_result = submit_to_kubernetes(func, final_args...)
            actual_result = func(*final_args, **final_kwargs) # Local run for demo
            
            # E. UPDATE FUTURE
            # Update the future so children can access this result
            self._future_registry[task_id]._set_result(actual_result)

    def print_graph(self, visualize=False):
        """
        Prints the task graph. 
        If visualize=True, it opens a window with the actual graph diagram.
        """
        print("\n=== Task Dependency Graph ===")
        if self.task_graph.number_of_nodes() == 0:
            print("  (Graph is empty)")
            return

        # 1. Text Representation (T0 -> T1)
        # We sort topologically so it prints in roughly the order of execution
        try:
            sorted_nodes = list(nx.topological_sort(self.task_graph))
        except nx.NetworkXUnfeasible:
            print("  ! Cyclic dependency detected (Graph cannot be resolved) !")
            sorted_nodes = self.task_graph.nodes()

        for node in sorted_nodes:
            parents = list(self.task_graph.predecessors(node))
            
            if not parents:
                print(f"  [Start] -> {node}")
            else:
                # Format: T0, T2 -> T1
                parents_str = ", ".join(parents)
                print(f"  {parents_str} -> {node}")

        # 2. Visual Representation (Matplotlib)
        if visualize:
            try:
                plt.figure(figsize=(8, 6))
                
                # --- START NEW LAYOUT LOGIC ---
                # 1. Assign "layers" to each node based on their depth in the graph
                #    Roots are layer 0, their children are layer 1, etc.
                for layer, nodes in enumerate(nx.topological_generations(self.task_graph)):
                    for node in nodes:
                        self.task_graph.nodes[node]["layer"] = layer
                
                # 2. Use multipartite_layout (Positions nodes in vertical columns by default)
                #    We align them by the 'layer' attribute we just created.
                pos = nx.multipartite_layout(self.task_graph, subset_key="layer")
                
                # 3. Flip the graph! (Multipartite is Left->Right, we want Top->Down)
                #    We swap (x, y) and negate x to rotate 90 degrees.
                for node, (x, y) in pos.items():
                    pos[node] = (y, -x) 
                # --- END NEW LAYOUT LOGIC ---

                nx.draw(
                    self.task_graph, pos, 
                    with_labels=True, 
                    node_color='lightblue', 
                    node_size=2000, 
                    font_size=10, 
                    font_weight='bold', 
                    arrows=True
                )
                plt.title("Saga Task Graph")
                plt.show()
            except ImportError:
                print("\n  (Install 'matplotlib' to see the visual graph diagram)")
            except Exception as e:
                print(f"Visualization error: {e}")