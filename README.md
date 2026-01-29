# SAGA + Parsl Integration

Advanced task scheduling for Parsl using SAGA algorithms to control task-to-node placement in Kubernetes clusters.

## Project Structure

```
scheduling/                    # Main project repository
├── parsl/                    # Git submodule: Modified Parsl fork
├── saga/                     # Git submodule: SAGA scheduling library
├── scripts/                  # Test scripts and examples
│   ├── test_local.py        # Local test without K8s
│   ├── test.py              # K8s integration test
│   └── test_multinode.py    # Multi-node K8s distribution test
├── pyproject.toml           # Main project configuration
└── README.md                # This file
```

## Overview

This project integrates SAGA (Scheduling Algorithms Gathered) with Parsl's High Throughput Executor to enable intelligent task placement on Kubernetes nodes based on:
- Task dependencies and DAG structure
- Network topology
- Computation and communication costs
- Scheduling algorithms (HEFT, CPOP, etc.)

### How It Works

1. **Task Graph Building**: Parsl builds a dependency graph of tasks
2. **SAGA Scheduling**: SAGA scheduler generates optimal task-to-node mapping
3. **Interchange Routing**: Custom routing in Parsl's interchange sends tasks to assigned nodes
4. **Node Matching**: Flexible hostname matching (exact, substring, or K8s node name)

## Setup

### Prerequisites

- Python 3.12+
- UV package manager
- Docker (for building worker images)
- kind or other Kubernetes cluster (for full integration tests)

### Installation

1. **Clone with submodules**:
```bash
git clone --recurse-submodules <your-repo-url>
cd scheduling
```

Or if you already cloned without submodules:
```bash
git submodule init
git submodule update
```

2. **Install with UV**:
```bash
uv sync
```

This installs both `parsl` and `saga` as editable local packages.

### Verifying Installation

```bash
uv run python -c "import parsl; import saga; print('✓ Ready!')"
```

## Running Tests

### Local Test (No K8s Required)

Tests SAGA scheduler integration with ThreadPoolExecutor:

```bash
uv run python scripts/test_local.py
```

Expected output:
- SAGA task graph built successfully
- Task assignments generated
- All 4 tasks assigned to nodes
- Tasks execute correctly

### Kubernetes Integration Tests

#### Basic K8s Test

Tests SAGA scheduler with K8s worker pods:

```bash
uv run python scripts/test.py
```

#### Multi-Node Distribution Test

Tests SAGA distributing tasks across multiple K8s nodes with varying task costs:

```bash
uv run python scripts/test_multinode.py
```

**K8s Setup:**

1. Create a multi-node kind cluster:
```bash
kind create cluster --config kind-config.yaml --name saga-test
```

2. Build the worker Docker image:
```bash
docker build --platform linux/amd64 -t parsl-saga-worker:latest -f Dockerfile.worker .
kind load docker-image parsl-saga-worker:latest --name saga-test
```

Expected output:
- `"SAGA: Task X assigned to node Y"`
- Tasks distributed across multiple worker nodes
- Summary showing which pods executed each task

## Implementation Details

### Modified Files

**Parsl (`parsl/`):**
- `dataflow/dflow.py`: SAGA scheduler integration, task graph building, assignment storage, closure bug fix
- `executors/high_throughput/executor.py`: Updated launch commands to use `-m` module syntax, added `_saga_node_affinity` to resource spec, added `encrypted` parameter support
- `executors/high_throughput/interchange.py`: SAGA affinity-based task routing to specific worker nodes
- `providers/kubernetes/kube.py`: Network graph query from K8s cluster nodes (not pods), added `image_pull_policy` parameter
- `config.py`: Added `saga_scheduler` configuration parameter

**SAGA (`saga/`):**
- No modifications needed - used as-is

**Infrastructure:**
- `Dockerfile.worker`: Multi-arch worker image with Parsl + SAGA dependencies
- `kind-config.yaml`: 3-worker Kubernetes cluster configuration
- `.vscode/settings.json`: Python path configuration for IDE

### Key Design Decisions

1. **Works with Parsl's Architecture**: Routes tasks to existing worker pools (not per-task pods)
2. **Lazy Execution**: Explicitly call `dfk.execute()` to trigger SAGA scheduling
3. **Flexible Matching**: Matches SAGA node names to manager hostnames (exact/substring/k8s)
4. **HTEx Only**: SAGA affinity only applied to HighThroughputExecutor (skipped for ThreadPoolExecutor)

## Development

### Working on Submodules

To make changes to Parsl or SAGA:

```bash
# Navigate to submodule
cd parsl  # or cd saga

# Make changes, commit
git add .
git commit -m "Your changes"

# Push to your fork
git push

# Return to main repo and update submodule reference
cd ..
git add parsl  # or saga
git commit -m "Update parsl submodule"
```

### Adding New Scripts

Place all test scripts and examples in `scripts/`:

```bash
# Create new script
cat > scripts/my_test.py << 'EOF'
import parsl
from saga.schedulers.heft import HeftScheduler
# Your code here
EOF

# Run with UV
uv run python scripts/my_test.py
```

## Current Status

✅ Implementation complete
✅ Local testing successful (ThreadPoolExecutor)
✅ K8s integration working (HighThroughputExecutor)
✅ Multi-node task distribution verified
✅ SAGA HEFT scheduler generating optimal assignments
✅ Task affinity routing to specific worker nodes

## Key Achievements

- **Closure Bug Fix**: Fixed critical callback capture issue in `dflow.py` that caused task execution to hang
- **Network Graph Discovery**: K8s provider correctly queries cluster nodes for SAGA network topology
- **Module Launch Commands**: Updated HTEx to use `-m` syntax for editable installs
- **Multi-node Validation**: Demonstrated tasks distributed across 3 worker pods with varying costs
- **Docker Multi-arch Support**: Worker image builds for x86_64 with all dependencies

## Next Steps

1. Add task cost estimation or user-specified weights for better SAGA scheduling
2. Benchmark performance vs default Parsl scheduling
3. Test with larger DAGs and more complex topologies
4. Explore other SAGA schedulers (CPOP, etc.)

## References

- [Parsl Documentation](https://parsl.readthedocs.io/)
- [SAGA GitHub](https://github.com/ANRGUSC/saga)
- [Modified Parsl Fork](https://github.com/kubishi/parsl)

## Contributing

This is a research project. For questions or contributions, please open an issue.

## License

- Parsl: Apache 2.0
- SAGA: MIT
- This integration: TBD
