import logging
import multiprocessing as mp
import os
from abc import ABC, abstractmethod
from collections import deque
from concurrent.futures import Future, ProcessPoolExecutor, wait, FIRST_COMPLETED
from functools import wraps
from typing import Any, Callable, ClassVar

import networkx as nx
from pydantic import BaseModel, Field, PrivateAttr
from saga import Network, Schedule, Scheduler, TaskGraph

logger = logging.getLogger(__name__)

DEFAULT_TASK_COST: float = 1.0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve(arg: Any) -> Any:
    """Replace an :class:`AppFuture` with its computed result; pass through otherwise."""
    return arg.result() if isinstance(arg, AppFuture) else arg


# ---------------------------------------------------------------------------
# AppFuture
# ---------------------------------------------------------------------------

class AppFuture(BaseModel):
    """Lazy placeholder for a task result.

    Created when a ``@python_app``-decorated function is called.  The actual
    result is populated later by the executor during :meth:`Executor.execute`.
    """

    model_config = {"arbitrary_types_allowed": True}

    task_id: str
    _result: Any = PrivateAttr(default=None)
    _done: bool = PrivateAttr(default=False)

    def result(self) -> Any:
        """Return the computed result, or raise if not yet executed."""
        if not self._done:
            raise RuntimeError(
                f"Task {self.task_id!r} not yet executed. "
                "Call executor.execute() first."
            )
        return self._result

    def done(self) -> bool:
        """Return whether the task has completed."""
        return self._done

    def _set_result(self, value: Any) -> None:
        self._result = value
        self._done = True

    def __repr__(self) -> str:
        status = repr(self._result) if self._done else "pending"
        return f"AppFuture({self.task_id!r}, {status})"


# ---------------------------------------------------------------------------
# Internal task record
# ---------------------------------------------------------------------------

class TaskRecord(BaseModel):
    """Callable, arguments, and metadata for a single registered task."""

    model_config = {"arbitrary_types_allowed": True}

    func: Callable[..., Any]
    func_key: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    cost: float


# Type alias: a function that submits one task and returns a Future.
#   (node_name, record, resolved_args, resolved_kwargs) -> Future
SubmitFn = Callable[[str, TaskRecord, tuple[Any, ...], dict[str, Any]], Future[Any]]


# ---------------------------------------------------------------------------
# Base Executor
# ---------------------------------------------------------------------------

class Executor(BaseModel, ABC):
    """Build a task DAG via ``@python_app`` and execute with SAGA scheduling.

    Subclasses implement:
        :meth:`get_network`
            Define available compute resources as a SAGA :class:`Network`.
        :meth:`execute`
            Schedule and execute tasks.  Typically calls :meth:`_dispatch`
            after setting up backend-specific resources (process pool,
            Kubernetes client, etc.).
    """

    model_config = {"arbitrary_types_allowed": True}

    # Shared function registry for cross-process dispatch.  Populated by
    # python_app() at decoration time.  Child processes re-import the module
    # which re-runs the decorators, so the table is available in every process.
    _func_table: ClassVar[dict[str, Callable[..., Any]]] = {}

    _graph: nx.DiGraph = PrivateAttr(default_factory=nx.DiGraph)
    _tasks: dict[str, TaskRecord] = PrivateAttr(default_factory=dict)
    _futures: dict[str, AppFuture] = PrivateAttr(default_factory=dict)
    _counter: int = PrivateAttr(default=0)

    # -- Decorator ----------------------------------------------------------

    def python_app(
        self, cost: float = DEFAULT_TASK_COST
    ) -> Callable[[Callable[..., Any]], Callable[..., AppFuture]]:
        """Register a function as a task node in the graph.

        Each *call* to the decorated function creates a graph node and returns
        an :class:`AppFuture`.  Any :class:`AppFuture` arguments become
        dependency edges.  The function body is deferred until
        :meth:`execute`.

        Args:
            cost: Computational cost hint for the SAGA scheduler.
                Defaults to :data:`DEFAULT_TASK_COST` (1.0).
        """

        def decorator(func: Callable[..., Any]) -> Callable[..., AppFuture]:
            func_key = f"{func.__module__}.{func.__qualname__}"
            Executor._func_table[func_key] = func

            @wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> AppFuture:
                task_id = f"task_{self._counter}"
                self._counter += 1

                # Discover dependencies from AppFuture arguments
                deps = [
                    a for a in (*args, *kwargs.values())
                    if isinstance(a, AppFuture)
                ]

                self._graph.add_node(task_id, name=func.__name__, cost=cost)
                for dep in deps:
                    self._graph.add_edge(dep.task_id, task_id, weight=0.0)

                self._tasks[task_id] = TaskRecord(
                    func=func,
                    func_key=func_key,
                    args=args,
                    kwargs=kwargs,
                    cost=cost,
                )

                future = AppFuture(task_id=task_id)
                self._futures[task_id] = future
                return future

            return wrapper

        return decorator

    # -- SAGA integration ---------------------------------------------------

    def build_saga_task_graph(self) -> TaskGraph:
        """Convert the internal ``nx.DiGraph`` to a SAGA :class:`TaskGraph`."""
        return TaskGraph.from_nx(self._graph, node_weight_attr="cost")

    @abstractmethod
    def get_network(self) -> Network:
        """Return the SAGA :class:`Network` describing available compute resources."""
        ...

    @abstractmethod
    def execute(self, scheduler: Scheduler) -> None:
        """Schedule and execute all registered tasks.

        Subclasses own the resource lifecycle (pool creation, K8s client
        setup, etc.) and typically call :meth:`_dispatch` with a
        backend-specific *submit_fn*.
        """
        ...

    # -- Dispatch -----------------------------------------------------------

    def _dispatch(self, schedule: Schedule, submit_fn: SubmitFn) -> None:
        """SAGA-aware dispatch loop with per-node ordering and dependency tracking.

        This is the core execution engine.  It respects:
          - **Node assignment**: each task runs on its SAGA-assigned node.
          - **Per-node ordering**: tasks on the same node execute in the order
            the scheduler specified (sorted by start time).
          - **Dependencies**: a task cannot start until all DAG predecessors
            have completed.
          - Exact start/end times are *not* enforced — only ordering and
            placement.

        SAGA may insert ``__super_source__`` / ``__super_sink__`` sentinel
        tasks into the schedule; these are filtered out automatically.

        Args:
            schedule: A SAGA :class:`Schedule` (node -> sorted ScheduledTask list).
            submit_fn: Backend-specific callable that submits a single task.
                Signature: ``(node_name, record, resolved_args, resolved_kwargs) -> Future``.
        """
        # -- 1. Build per-node task queues from the schedule -----------------
        node_queues: dict[str, deque[str]] = {}
        task_to_node: dict[str, str] = {}

        for node_name, scheduled_tasks in schedule.items():
            queue: deque[str] = deque()
            for st in scheduled_tasks:
                if st.name.startswith("__super_"):
                    continue
                queue.append(st.name)
                task_to_node[st.name] = node_name
            if queue:
                node_queues[node_name] = queue

        if not node_queues:
            return

        # -- 2. Tracking state -----------------------------------------------
        completed: set[str] = set()
        running: dict[Future[Any], str] = {}
        idle_nodes: set[str] = set(node_queues.keys())

        def _deps_met(task_id: str) -> bool:
            return all(
                pred in completed for pred in self._graph.predecessors(task_id)
            )

        def _try_dispatch() -> None:
            newly_busy: list[str] = []
            for node_name in list(idle_nodes):
                queue = node_queues[node_name]
                if not queue:
                    continue
                task_id = queue[0]
                if not _deps_met(task_id):
                    continue

                queue.popleft()
                newly_busy.append(node_name)

                record = self._tasks[task_id]
                resolved_args = tuple(_resolve(a) for a in record.args)
                resolved_kwargs = {k: _resolve(v) for k, v in record.kwargs.items()}

                fut = submit_fn(node_name, record, resolved_args, resolved_kwargs)
                running[fut] = task_id
                logger.info(
                    "Dispatched %s (%s) -> %s",
                    task_id, record.func.__name__, node_name,
                )

            for name in newly_busy:
                idle_nodes.discard(name)

        # -- 3. Main event loop ----------------------------------------------
        _try_dispatch()

        while running:
            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for fut in done:
                task_id = running.pop(fut)
                result = fut.result()  # propagates worker exceptions
                self._futures[task_id]._set_result(result)
                completed.add(task_id)
                idle_nodes.add(task_to_node[task_id])
                logger.info(
                    "Completed %s -> %r (%s)",
                    task_id, result, task_to_node[task_id],
                )

            _try_dispatch()

        # Sanity check
        unfinished = set(self._tasks) - completed
        if unfinished:
            raise RuntimeError(f"Dispatch ended with unfinished tasks: {unfinished}")

    # -- Schedule logging ---------------------------------------------------

    def _log_schedule(self, schedule: Schedule) -> None:
        """Log the SAGA schedule for debugging."""
        logger.info("SAGA schedule (makespan=%.2f):", schedule.makespan)
        for node_name, scheduled_tasks in schedule.items():
            for st in scheduled_tasks:
                if not st.name.startswith("__super_"):
                    logger.info(
                        "  %s -> %s [%.2f - %.2f]",
                        st.name, node_name, st.start, st.end,
                    )

    # -- Visualization ------------------------------------------------------

    def print_graph(self, visualize: bool = False) -> None:
        """Print the task dependency graph.

        Args:
            visualize: If *True*, render an interactive matplotlib diagram.
        """
        print("\n=== Task Dependency Graph ===")
        if not self._graph.nodes:
            print("  (empty)")
            return

        try:
            order = list(nx.topological_sort(self._graph))
        except nx.NetworkXUnfeasible:
            print("  ! Cycle detected !")
            order = list(self._graph.nodes())

        for node in order:
            data = self._graph.nodes[node]
            label = f"{node} ({data.get('name', node)}, cost={data.get('cost', 0.0)})"
            parents = list(self._graph.predecessors(node))
            if parents:
                print(f"  {', '.join(parents)} -> {label}")
            else:
                print(f"  [root] -> {label}")

        if not visualize:
            return

        try:
            import matplotlib.pyplot as plt
        except ImportError:
            print("  (install matplotlib for visual diagrams)")
            return

        plt.figure(figsize=(8, 6))
        for layer, nodes in enumerate(nx.topological_generations(self._graph)):
            for n in nodes:
                self._graph.nodes[n]["layer"] = layer
        pos = nx.multipartite_layout(self._graph, subset_key="layer")
        pos = {n: (y, -x) for n, (x, y) in pos.items()}
        nx.draw(
            self._graph, pos,
            with_labels=True,
            node_color="lightblue",
            node_size=2000,
            font_size=10,
            font_weight="bold",
        )
        plt.title("Task Dependency Graph")
        plt.show()


# ---------------------------------------------------------------------------
# Cross-process function dispatch
#
# ProcessPoolExecutor pickles callables by __module__.__qualname__.  The
# @wraps decorator copies these from the inner function to the wrapper,
# so pickle would resolve to the wrapper in the child process.  Sending
# a string key and looking up in Executor._func_table sidesteps this.
# ---------------------------------------------------------------------------

def _pin_worker(core_id: int) -> None:
    """ProcessPoolExecutor initializer: pin the worker process to a CPU core.

    Uses ``os.sched_setaffinity`` (Linux).  Silently skipped on platforms
    that do not support CPU affinity (macOS, Windows).
    """
    try:
        os.sched_setaffinity(0, {core_id})
    except (AttributeError, OSError):
        pass


def _worker_call(
    func_key: str, args: tuple[Any, ...], kwargs: dict[str, Any]
) -> Any:
    """Subprocess entry-point: resolve *func_key* via the executor func table."""
    return Executor._func_table[func_key](*args, **kwargs)


# ---------------------------------------------------------------------------
# MultiprocessingExecutor
# ---------------------------------------------------------------------------

class MultiprocessingExecutor(Executor):
    """Execute tasks across CPU cores using :class:`ProcessPoolExecutor`.

    Each CPU core is modeled as a SAGA network node.  The SAGA scheduler
    determines which core runs which task and in what order; the dispatch
    loop enforces these assignments.

    A dedicated single-worker :class:`ProcessPoolExecutor` is created per
    core so that each worker process can be pinned to its assigned CPU via
    ``os.sched_setaffinity`` (Linux).  On platforms without CPU affinity
    support the workers still function correctly, just without pinning.

    Note:
        Task functions must be defined at **module level** (not inside
        ``if __name__ == '__main__':``).  Their arguments must be picklable.

    Args:
        max_workers: Number of CPU cores to use.  Defaults to all available.
    """

    max_workers: int = Field(default_factory=lambda: mp.cpu_count() or 1)

    def get_network(self) -> Network:
        """One node per CPU core, fully connected with uniform speed."""
        n = self.max_workers
        return Network.create(
            nodes=[(f"core_{i}", 1.0) for i in range(n)],
            edges=[
                (f"core_{i}", f"core_{j}", 1.0)
                for i in range(n)
                for j in range(i + 1, n)
            ],
        )

    def execute(self, scheduler: Scheduler) -> None:
        """Schedule with SAGA, then dispatch across per-core process pools.

        Creates one single-worker :class:`ProcessPoolExecutor` per CPU core,
        each pinned via :func:`_pin_worker`.  The ``submit_fn`` routes each
        task to the pool corresponding to its SAGA-assigned node.

        Args:
            scheduler: A SAGA :class:`Scheduler` instance (e.g. ``HeftScheduler``).
        """
        if not self._graph.nodes:
            logger.info("No tasks to execute.")
            return

        network = self.get_network()
        saga_tg = self.build_saga_task_graph()
        schedule = scheduler.schedule(network, saga_tg)
        self._log_schedule(schedule)

        # One single-worker pool per core, pinned to that CPU.
        pools: dict[str, ProcessPoolExecutor] = {}
        try:
            for i in range(self.max_workers):
                name = f"core_{i}"
                pools[name] = ProcessPoolExecutor(
                    max_workers=1,
                    initializer=_pin_worker,
                    initargs=(i,),
                )

            def submit_fn(
                node_name: str,
                record: TaskRecord,
                args: tuple[Any, ...],
                kwargs: dict[str, Any],
            ) -> Future[Any]:
                return pools[node_name].submit(
                    _worker_call, record.func_key, args, kwargs,
                )

            self._dispatch(schedule, submit_fn)
        finally:
            for pool in pools.values():
                pool.shutdown(wait=True)


# ---------------------------------------------------------------------------
# KubernetesExecutor (skeleton)
# ---------------------------------------------------------------------------

class KubernetesExecutor(Executor):
    """Execute tasks on Kubernetes worker nodes with SAGA scheduling.

    Each K8s worker node is modeled as a SAGA network node.  Tasks are
    submitted as Kubernetes Jobs with ``nodeSelector`` to enforce SAGA
    node assignments.

    Args:
        namespace: K8s namespace for job creation.
        image: Container image that can execute serialized Python tasks.
        image_pull_policy: K8s image pull policy.
        kubeconfig: Path to kubeconfig file, or ``None`` for in-cluster config.
    """

    namespace: str = "default"
    image: str = "parsl-saga-worker:latest"
    image_pull_policy: str = "IfNotPresent"
    kubeconfig: str | None = None

    def get_network(self) -> Network:
        """Query K8s cluster nodes to build a SAGA :class:`Network`.

        Returns one SAGA node per K8s worker node (control-plane nodes are
        excluded).  All nodes are assigned uniform speed and are fully
        connected.
        """
        from kubernetes import client, config

        if self.kubeconfig:
            config.load_kube_config(config_file=self.kubeconfig)
        else:
            config.load_incluster_config()

        v1 = client.CoreV1Api()
        k8s_nodes = v1.list_node().items

        worker_nodes: list[str] = []
        for node in k8s_nodes:
            labels = node.metadata.labels or {}
            if "node-role.kubernetes.io/control-plane" not in labels:
                worker_nodes.append(node.metadata.name)

        if not worker_nodes:
            raise RuntimeError("No worker nodes found in K8s cluster.")

        n = len(worker_nodes)
        return Network.create(
            nodes=[(name, 1.0) for name in worker_nodes],
            edges=[
                (worker_nodes[i], worker_nodes[j], 1.0)
                for i in range(n)
                for j in range(i + 1, n)
            ],
        )

    def execute(self, scheduler: Scheduler) -> None:
        """Schedule with SAGA, then dispatch as K8s Jobs.

        Args:
            scheduler: A SAGA :class:`Scheduler` instance.

        Raises:
            NotImplementedError: K8s job submission is not yet implemented.
        """
        if not self._graph.nodes:
            logger.info("No tasks to execute.")
            return

        network = self.get_network()
        saga_tg = self.build_saga_task_graph()
        schedule = scheduler.schedule(network, saga_tg)
        self._log_schedule(schedule)

        # TODO: Implement K8s job submission.
        #
        # The submit_fn should:
        #   1. Serialize the function + resolved args (e.g. cloudpickle)
        #   2. Create a K8s Job with:
        #      - nodeSelector: {"kubernetes.io/hostname": node_name}
        #      - Container image: self.image
        #      - Serialized payload mounted via ConfigMap or Volume
        #   3. Watch/poll the Job for completion
        #   4. Deserialize the result from the Job's output
        #   5. Return a concurrent.futures.Future that resolves with the result
        #
        # Example submit_fn skeleton:
        #
        #   def submit_fn(node_name, record, args, kwargs):
        #       fut = concurrent.futures.Future()
        #       # ... create Job, start watcher thread ...
        #       return fut
        #
        #   self._dispatch(schedule, submit_fn)

        raise NotImplementedError(
            "KubernetesExecutor.execute() is not yet implemented. "
            "See TODO comments above for the implementation plan."
        )
