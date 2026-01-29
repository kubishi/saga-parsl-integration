# SAGA + Parsl Integration Project Guide

This guide helps Claude Code sessions understand and work with the SAGA + Parsl integration project.

## Project Overview

This project integrates the SAGA (Scheduling Algorithms Gathered) library with Parsl's High Throughput Executor to enable intelligent task scheduling in distributed computing environments. SAGA's HEFT algorithm determines optimal task-to-node assignments based on task dependencies and network topology, which Parsl then enforces during execution.

## Architecture

### Components

1. **SAGA Library** (`saga/` submodule)
   - Provides scheduling algorithms (HEFT, CPOP, etc.)
   - Takes task graphs and network graphs as input
   - Outputs task-to-node assignments with start/end times

2. **Parsl Framework** (`parsl/` submodule - modified fork)
   - Provides parallel workflow execution framework
   - Modified to integrate SAGA scheduler
   - Enforces SAGA assignments via task affinity routing

3. **Integration Layer**
   - Lives in `parsl/parsl/dataflow/dflow.py`
   - Builds SAGA-compatible task and network graphs
   - Stores assignments and passes them to executor

### Execution Flow

```
1. User creates Parsl apps and tasks
2. Tasks registered with DataFlowKernel (lazy execution)
3. User calls dfk.execute() to trigger scheduling
4. DFK builds task graph from dependencies
5. DFK queries executor's provider for network graph
6. SAGA scheduler generates optimal assignments
7. Assignments stored in dfk._saga_task_assignments
8. Tasks launched with _saga_node_affinity resource spec
9. HTEx routes tasks to assigned nodes
10. Workers execute tasks and return results
```

## Critical Implementation Details

### 1. Closure Bug Fix (dflow.py:1158-1167)

**Problem**: Task execution hung after first tasks completed
**Root Cause**: Callback captured task_record by reference, all callbacks pointed to last task
**Solution**: Use default parameter to capture by value

```python
for task_record in self.tasks.values():
    for dependency in task_record['depends']:
        def callback_adapter(dep_fut: Future, tr=task_record) -> None:
            self.launch_if_ready(tr)
        dependency.add_done_callback(callback_adapter)
    self.launch_if_ready(task_record)
```

### 2. Network Graph Discovery (providers/kubernetes/kube.py:get_graph_nodes)

**Problem**: Network graph had 0 nodes, StopIteration error
**Root Cause**: Queried for pods which don't exist when execute() is called
**Solution**: Query K8s cluster nodes instead (exist before pods created)

```python
def get_graph_nodes(self) -> List[Dict[str, Any]]:
    node_list = self.kube_client.list_node()  # NOT list_namespaced_pod!
    # Process node info...
```

### 3. Module Launch Commands (executors/high_throughput/executor.py)

**Problem**: interchange.py and process_worker_pool.py not found on PATH
**Root Cause**: Editable installs don't put scripts on PATH
**Solution**: Use `python -m` module syntax

```python
DEFAULT_INTERCHANGE_LAUNCH_CMD = [sys.executable, "-m", "parsl.executors.high_throughput.interchange"]
DEFAULT_LAUNCH_CMD = "python3 -m parsl.executors.high_throughput.process_worker_pool ..."
```

### 4. SAGA Affinity Resource Spec (executors/high_throughput/executor.py:391)

**Problem**: InvalidResourceSpecification for _saga_node_affinity
**Solution**: Add to acceptable_fields set

```python
acceptable_fields = {'priority', '_saga_node_affinity'}
```

### 5. Encryption for Local Testing (config parameter)

**Problem**: CurveZMQ certificate errors in containerized environments
**Solution**: Add `encrypted=False` parameter for local testing

```python
config = Config(
    executors=[HighThroughputExecutor(
        encrypted=False,  # Disable for local testing
        # ...
    )]
)
```

## File Organization

```
/home/jared/projects/scheduling/
├── parsl/                          # Modified Parsl fork (submodule)
│   └── parsl/
│       ├── dataflow/dflow.py       # SAGA integration + closure fix
│       ├── executors/high_throughput/
│       │   └── executor.py         # Launch commands + resource spec
│       ├── providers/kubernetes/
│       │   └── kube.py             # Network graph from cluster nodes
│       └── config.py               # saga_scheduler parameter
├── saga/                           # SAGA library (submodule)
├── scripts/                        # Test scripts
│   ├── test_local.py              # ThreadPoolExecutor test
│   ├── test.py                    # Basic K8s test
│   └── test_multinode.py          # Multi-node distribution test
├── Dockerfile.worker              # Worker image with Parsl + SAGA
├── kind-config.yaml               # 3-worker K8s cluster config
├── pyproject.toml                 # Main project config
├── README.md                      # User-facing documentation
└── CLAUDE.md                      # This file
```

## Common Tasks

### Running Tests

1. **Local test** (no K8s): `uv run python scripts/test_local.py`
2. **K8s test**: `uv run python scripts/test.py`
3. **Multi-node test**: `uv run python scripts/test_multinode.py`

### Working with Submodules

```bash
# Make changes in submodule
cd parsl  # or saga
git checkout -b feature/my-feature
# ... make changes ...
git commit -m "description"
git push origin feature/my-feature

# Update main repo reference
cd ..
git add parsl  # or saga
git commit -m "Update parsl submodule"
```

### Building Docker Images

```bash
# Build worker image (x86_64)
docker build --platform linux/amd64 -t parsl-saga-worker:latest -f Dockerfile.worker .

# Load into kind cluster
kind load docker-image parsl-saga-worker:latest --name saga-test
```

### Setting up K8s Cluster

```bash
# Create multi-node kind cluster
kind create cluster --config kind-config.yaml --name saga-test

# Verify nodes
kubectl get nodes

# Load worker image
kind load docker-image parsl-saga-worker:latest --name saga-test
```

## Debugging Tips

### Task Execution Hangs

- Check for closure bugs (capturing loop variables by reference)
- Verify all dependencies are registered correctly
- Look for `launch_if_ready` being called with wrong task_record

### Network Graph is Empty

- Verify provider implements `get_graph_nodes()`
- Check that cluster nodes exist (not just pods)
- Look for `"Network graph: Graph with 0 nodes"` in logs

### Tasks Not Distributed

- Check SAGA task assignments: `dfk._saga_task_assignments`
- Verify `_saga_node_affinity` in resource_spec
- Check interchange logs for routing decisions
- Ensure HighThroughputExecutor (not ThreadPoolExecutor)

### Import Errors

- Verify virtual environment activated: `.venv/bin/python`
- Check VSCode settings: `.vscode/settings.json` has correct paths
- Ensure `uv sync` completed successfully

### Docker Image Issues

- Verify platform: `--platform linux/amd64` for x86_64 systems
- Check all dependencies installed (pyzmq, typeguard, tblib, dill, etc.)
- Use `image_pull_policy='Never'` for local images

## Important Constraints

1. **SAGA affinity only works with HighThroughputExecutor** - ThreadPoolExecutor doesn't support node affinity
2. **Must call `dfk.execute()` explicitly** - Lazy execution means SAGA won't run until triggered
3. **Network graph must exist before execute()** - Query cluster nodes, not pods
4. **Task costs are uniform by default** - All tasks get cost=1.0 unless custom weights specified
5. **Editable installs require `-m` syntax** - Scripts aren't on PATH, use module execution

## Testing Patterns

### Creating Good Test Cases

Use diamond-shaped DAGs with varying costs to ensure multi-node distribution:

```python
# t1 (light - 0.5s)
#  ├── t2 (heavy - 2.0s)
#  └── t3 (medium - 1.0s)
#       └── t4 (light - 0.5s, depends on t2 and t3)
```

This ensures SAGA has optimization opportunities and will distribute work.

### Verifying Distribution

1. Check SAGA assignments in logs: `"SAGA: Task X assigned to node Y"`
2. Check actual execution: Each task prints its hostname
3. Count unique hostnames in results
4. Compare total time vs expected parallel time

## Code Style and Conventions

- Use Parsl's existing logging infrastructure
- Follow Parsl's debug/info/warning log levels
- Add docstrings for new functions
- Keep changes minimal - modify existing code paths rather than adding new ones
- Use type hints where possible
- Test locally before K8s testing

## Git Workflow

### Branch Structure

- `main` - stable integration code
- `feature/saga-parsl-integration` - active development branch
- Submodules have their own branches on their respective forks

### Commit Messages

Follow conventional commits style:
- `feat: add SAGA scheduler integration to DFK`
- `fix: closure bug in task callback registration`
- `docs: update README with K8s setup instructions`

## Future Work

1. **Task Cost Estimation**: Add benchmarking or user-specified weights
2. **Alternative Schedulers**: Test CPOP and other SAGA algorithms
3. **Performance Benchmarking**: Compare SAGA vs default scheduling
4. **Larger Scale Testing**: Test with 100+ tasks across 10+ nodes
5. **Dynamic Rescheduling**: Handle task failures and node changes

## Resources

- [Parsl Documentation](https://parsl.readthedocs.io/)
- [SAGA GitHub](https://github.com/ANRGUSC/saga)
- [HEFT Algorithm Paper](https://doi.org/10.1109/71.993206)
- [Claude Code Skills Documentation](https://code.claude.com/docs/en/skills)

## Questions to Ask

When starting a new session, ask:

1. What is the current state of tests? (run test_local.py to verify)
2. Are there any failing tests that need investigation?
3. What new functionality is being requested?
4. Are we modifying Parsl, SAGA, or the integration layer?
5. Do changes need testing in both local and K8s environments?
