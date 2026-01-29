# SAGA + Parsl Integration Project

Integration of SAGA scheduling algorithms with Parsl's High Throughput Executor for intelligent task placement on Kubernetes nodes.

## Project Structure

- `parsl/` - Modified Parsl fork (submodule on `feature/saga-parsl-integration` branch)
- `saga/` - SAGA library (submodule, unmodified)
- `scripts/test_local.py` - Local test with ThreadPoolExecutor
- `scripts/test.py` - Basic K8s test
- `scripts/test_multinode.py` - Multi-node distribution test

## Common Commands

```bash
# Run tests
uv run python scripts/test_local.py      # Local (no K8s)
uv run python scripts/test.py            # K8s basic
uv run python scripts/test_multinode.py  # Multi-node

# K8s cluster setup
kind create cluster --config kind-config.yaml --name saga-test
docker build --platform linux/amd64 -t parsl-saga-worker:latest -f Dockerfile.worker .
kind load docker-image parsl-saga-worker:latest --name saga-test

# Submodule workflow
cd parsl && git checkout feature/saga-parsl-integration
cd saga && git checkout main
```

## Critical Bug Patterns

### Closure Capture Bug (dflow.py:1158-1167)
- **Always** use default parameters to capture loop variables by value
- Bad: `lambda: use task_record`
- Good: `lambda tr=task_record: use tr`

### Network Graph Discovery (providers/kubernetes/kube.py:get_graph_nodes)
- Query K8s cluster nodes (`list_node()`), NOT pods (`list_namespaced_pod()`)
- Cluster nodes exist before execute() is called; pods don't

### Module Launch Commands
- Use `python -m module.path` syntax for editable installs
- Scripts aren't on PATH with editable pip installs

## Architecture

### Execution Flow
1. User creates tasks with `@python_app` decorator
2. Tasks registered in DataFlowKernel (lazy execution)
3. `dfk.execute()` triggers SAGA scheduling
4. SAGA scheduler generates task-to-node assignments
5. Assignments stored in `dfk._saga_task_assignments`
6. HTEx routes tasks via `_saga_node_affinity` resource spec

### Key Constraints
- SAGA affinity only works with HighThroughputExecutor (not ThreadPoolExecutor)
- Must call `dfk.execute()` explicitly for lazy execution
- Network graph must exist before execute() - query cluster nodes, not pods
- Task costs default to 1.0 (uniform) unless custom weights specified

## Modified Files

### Parsl Changes
- `dataflow/dflow.py` - SAGA integration, closure bug fix
- `executors/high_throughput/executor.py` - Module launch commands, `_saga_node_affinity` support
- `providers/kubernetes/kube.py` - Network graph from cluster nodes, `image_pull_policy` parameter
- `config.py` - `saga_scheduler` parameter

### Infrastructure
- `Dockerfile.worker` - x86_64 worker image with all dependencies
- `kind-config.yaml` - 3-worker K8s cluster
- `.vscode/settings.json` - Python path for IDE

## Testing Patterns

### Good Test DAGs
Use diamond shapes with varying costs for multi-node distribution:
```python
t1 (0.5s) -> [t2 (2.0s), t3 (1.0s)] -> t4 (0.5s)
```

### Verification
- Check SAGA assignments: `dfk._saga_task_assignments`
- Check logs: `"SAGA: Task X assigned to node Y"`
- Verify distribution: Count unique hostnames in results

## Code Conventions

- Use Parsl's logging infrastructure (debug/info/warning levels)
- Minimize changes - modify existing code paths rather than adding new ones
- Add type hints where possible
- Test locally before K8s testing

## Debugging

### Task Execution Hangs
- Check for closure bugs (loop variable capture)
- Verify `launch_if_ready` called with correct task_record

### Network Graph Empty
- Error: `"Network graph: Graph with 0 nodes"`
- Fix: Ensure provider queries cluster nodes, not pods

### Tasks Not Distributed
- Check `dfk._saga_task_assignments` exists
- Verify `_saga_node_affinity` in resource_spec
- Confirm using HighThroughputExecutor (not ThreadPoolExecutor)

### Import Errors
- Activate venv: `.venv/bin/python`
- Check `.vscode/settings.json` has correct paths
- Run `uv sync`

## Git Workflow

- Main repo: `kubishi/saga-parsl-integration`
- Parsl submodule: `kubishi/parsl` on `feature/saga-parsl-integration`
- SAGA submodule: `ANRGUSC/saga` on `main`
- Use conventional commits: `feat:`, `fix:`, `docs:`
